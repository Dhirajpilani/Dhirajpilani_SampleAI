#!/usr/bin/env python3
"""Daily global job aggregation and notification system.

Features:
- Fetch jobs from multiple sources (API-first, compliant by design)
- Filter by role/experience/date/remote
- Deduplicate across sources
- Rank jobs by relevance
- Persist in SQLite (optional)
- Send HTML email summary over SMTP
- Run once or as a scheduler service (7:00 AM IST)
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import html
import json
import logging
import os
import sqlite3
import ssl
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - optional fallback
    pd = None


# ---------- Configuration ----------
IST = timezone(timedelta(hours=5, minutes=30))
DEFAULT_KEYWORDS = [
    "R2R",
    "Record to Report",
    "Finance Transformation",
    "BlackLine",
    "Account Reconciliation",
    "Senior Analyst",
    "Manager",
]

HIGH_QUALITY_COMPANIES = {
    "Deloitte",
    "EY",
    "KPMG",
    "PwC",
    "Accenture",
    "Cognizant",
    "Infosys",
    "Genpact",
    "Capgemini",
    "Oracle",
    "SAP",
    "Microsoft",
    "Amazon",
    "Google",
}


@dataclass
class JobPost:
    source: str
    job_title: str
    company_name: str
    location: str
    description: str
    posted_date: datetime
    apply_link: str
    remote: bool = False
    raw_payload: Optional[str] = None
    relevance_score: float = 0.0

    def dedupe_key(self) -> str:
        key = "|".join(
            [
                self.job_title.strip().lower(),
                self.company_name.strip().lower(),
                self.location.strip().lower(),
            ]
        )
        return hashlib.sha256(key.encode("utf-8")).hexdigest()


class Config:
    def __init__(self) -> None:
        self.keywords = os.getenv("JOB_KEYWORDS", ",".join(DEFAULT_KEYWORDS)).split(",")
        self.remote_only = os.getenv("REMOTE_ONLY", "false").lower() == "true"
        self.max_top_jobs = int(os.getenv("MAX_TOP_JOBS", "10"))
        self.recipient_email = os.getenv("RECIPIENT_EMAIL", "")

        # SMTP
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = os.getenv("SMTP_USER", "")
        self.smtp_password = os.getenv("SMTP_PASSWORD", "")
        self.smtp_sender = os.getenv("SMTP_SENDER", self.smtp_user)

        # APIs
        self.serpapi_api_key = os.getenv("SERPAPI_API_KEY", "")

        # Runtime
        self.db_path = os.getenv("DB_PATH", "jobs.db")
        self.log_file = os.getenv("LOG_FILE", "job_alert.log")
        self.request_timeout_seconds = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
        self.retries = int(os.getenv("RETRIES", "3"))
        self.backoff_seconds = int(os.getenv("BACKOFF_SECONDS", "2"))


def setup_logging(log_file: str) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )


def resilient_session(total_retries: int, backoff_factor: int) -> requests.Session:
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "job-alert-bot/1.0 (respectful; contact: admin@example.com)"})
    return s


class BaseSource:
    source_name = "base"

    def fetch(self, cfg: Config, since_utc: datetime) -> list[JobPost]:
        raise NotImplementedError


class SerpAPISource(BaseSource):
    """Google Jobs results via SerpAPI (API-first, no direct scraping)."""

    source_name = "serpapi-google-jobs"

    def __init__(self, site_filter: Optional[str] = None):
        self.site_filter = site_filter

    def fetch(self, cfg: Config, since_utc: datetime) -> list[JobPost]:
        if not cfg.serpapi_api_key:
            logging.warning("SERPAPI_API_KEY missing; skipping %s", self.source_name)
            return []

        session = resilient_session(cfg.retries, cfg.backoff_seconds)
        results: list[JobPost] = []

        for keyword in cfg.keywords:
            query = keyword.strip()
            if self.site_filter:
                query = f"{query} site:{self.site_filter}"

            params = {
                "engine": "google_jobs",
                "q": query,
                "api_key": cfg.serpapi_api_key,
                "hl": "en",
            }

            try:
                resp = session.get(
                    "https://serpapi.com/search.json",
                    params=params,
                    timeout=cfg.request_timeout_seconds,
                )
                resp.raise_for_status()
                data = resp.json()
                for item in data.get("jobs_results", []):
                    posted_at = parse_posted_time(item.get("detected_extensions", {}).get("posted_at"))
                    if posted_at and posted_at < since_utc:
                        continue

                    title = item.get("title") or ""
                    company = item.get("company_name") or ""
                    location = item.get("location") or "Global"
                    description = (item.get("description") or "")[:450]
                    link = item.get("related_links", [{}])[0].get("link") or item.get("share_link") or ""
                    is_remote = "remote" in location.lower() or "remote" in description.lower()
                    if cfg.remote_only and not is_remote:
                        continue

                    if not title or not company or not link:
                        continue

                    results.append(
                        JobPost(
                            source=self.source_name,
                            job_title=title,
                            company_name=company,
                            location=location,
                            description=description,
                            posted_date=posted_at or datetime.now(timezone.utc),
                            apply_link=link,
                            remote=is_remote,
                            raw_payload=json.dumps(item),
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                logging.exception("Source error (%s, keyword=%s): %s", self.source_name, keyword, exc)

            time.sleep(1)

        return results


class RemotiveSource(BaseSource):
    """Reliable global jobs API used as an additional source."""

    source_name = "remotive"

    def fetch(self, cfg: Config, since_utc: datetime) -> list[JobPost]:
        session = resilient_session(cfg.retries, cfg.backoff_seconds)
        jobs: list[JobPost] = []

        for keyword in cfg.keywords:
            try:
                resp = session.get(
                    "https://remotive.com/api/remote-jobs",
                    params={"search": keyword.strip()},
                    timeout=cfg.request_timeout_seconds,
                )
                resp.raise_for_status()
                data = resp.json()
                for item in data.get("jobs", []):
                    published = parse_iso_datetime(item.get("publication_date"))
                    if not published or published < since_utc:
                        continue

                    location = item.get("candidate_required_location") or "Remote"
                    is_remote = True
                    if cfg.remote_only and not is_remote:
                        continue

                    jobs.append(
                        JobPost(
                            source=self.source_name,
                            job_title=item.get("title", ""),
                            company_name=item.get("company_name", ""),
                            location=location,
                            description=strip_html(item.get("description", ""))[:450],
                            posted_date=published,
                            apply_link=item.get("url", ""),
                            remote=is_remote,
                            raw_payload=json.dumps(item),
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                logging.exception("Source error (%s, keyword=%s): %s", self.source_name, keyword, exc)
            time.sleep(1)

        return jobs


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def parse_posted_time(text: Optional[str]) -> Optional[datetime]:
    """Parse relative posted text like '2 days ago', '12 hours ago'."""
    if not text:
        return None
    text = text.strip().lower()
    now = datetime.now(timezone.utc)

    try:
        num = int(text.split()[0])
    except (ValueError, IndexError):
        return None

    if "hour" in text:
        return now - timedelta(hours=num)
    if "day" in text:
        return now - timedelta(days=num)
    if "week" in text:
        return now - timedelta(days=7 * num)
    return None


def strip_html(text: str) -> str:
    return (
        text.replace("<br>", " ")
        .replace("<br/>", " ")
        .replace("</p>", " ")
        .replace("<p>", " ")
        .replace("&nbsp;", " ")
    )


def deduplicate_jobs(jobs: Iterable[JobPost]) -> list[JobPost]:
    uniq: dict[str, JobPost] = {}
    for job in jobs:
        if not job.job_title or not job.company_name or not job.apply_link:
            continue
        key = job.dedupe_key()
        if key not in uniq:
            uniq[key] = job
    return list(uniq.values())


def relevance_score(job: JobPost, keywords: list[str]) -> float:
    text = f"{job.job_title} {job.description}".lower()
    score = 0.0

    for kw in keywords:
        if kw.lower() in text:
            score += 2.0

    if "blackline" in text:
        score += 5.0
    if "record to report" in text or "r2r" in text:
        score += 4.0
    if "finance transformation" in text:
        score += 3.0
    if job.company_name in HIGH_QUALITY_COMPANIES:
        score += 2.5
    if "senior" in text or "manager" in text or "lead" in text:
        score += 1.5
    if job.remote:
        score += 0.5

    return score


def rank_and_filter(jobs: list[JobPost], cfg: Config, since_utc: datetime) -> list[JobPost]:
    filtered: list[JobPost] = []
    for job in jobs:
        if job.posted_date < since_utc:
            continue

        # Keep mid-senior-ish opportunities by textual heuristic
        mid_senior_hint = any(
            t in f"{job.job_title} {job.description}".lower()
            for t in ["senior", "manager", "lead", "analyst", "specialist"]
        )
        if not mid_senior_hint:
            continue

        job.relevance_score = relevance_score(job, cfg.keywords)
        filtered.append(job)

    return sorted(filtered, key=lambda j: (j.relevance_score, j.posted_date), reverse=True)


def build_html_email(top_jobs: list[JobPost], other_jobs: list[JobPost], run_date_ist: datetime) -> str:
    def render_job(job: JobPost) -> str:
        safe_title = html.escape(job.job_title)
        safe_company = html.escape(job.company_name)
        safe_loc = html.escape(job.location)
        safe_summary = html.escape((job.description or "No summary available")[:300])
        safe_link = html.escape(job.apply_link)
        posted = job.posted_date.astimezone(IST).strftime("%Y-%m-%d %H:%M IST")
        return f"""
        <div style='padding:10px 0;border-bottom:1px solid #eee;'>
            <div style='font-size:16px;font-weight:600;'>
                <a href='{safe_link}' style='text-decoration:none;color:#0a66c2;'>{safe_title}</a>
            </div>
            <div style='color:#444;margin-top:2px;'>{safe_company} • {safe_loc}</div>
            <div style='color:#666;margin-top:6px;line-height:1.4;'>{safe_summary}</div>
            <div style='font-size:12px;color:#999;margin-top:4px;'>Posted: {posted} | Source: {job.source}</div>
        </div>
        """

    top_html = "\n".join(render_job(j) for j in top_jobs) or "<p>No top jobs found today.</p>"
    others_html = "\n".join(render_job(j) for j in other_jobs) or "<p>No additional relevant opportunities.</p>"
    dt = run_date_ist.strftime("%d %b %Y")

    return f"""
    <html>
      <body style='font-family:Arial,sans-serif;'>
        <h2>Daily Global Job Alerts – {dt}</h2>
        <p>Automated scan for the last 24 hours across compliant data sources.</p>

        <h3 style='margin-top:24px;'>Top 10 Recommended Jobs</h3>
        {top_html}

        <h3 style='margin-top:24px;'>Other Relevant Opportunities</h3>
        {others_html}

        <p style='margin-top:24px;color:#666;font-size:12px;'>
          Keywords: R2R, Record to Report, Finance Transformation, BlackLine, Account Reconciliation,
          Senior Analyst, Manager.
        </p>
      </body>
    </html>
    """


def send_email(cfg: Config, subject: str, html_body: str) -> None:
    if not (cfg.recipient_email and cfg.smtp_user and cfg.smtp_password and cfg.smtp_sender):
        raise ValueError("Missing SMTP or recipient configuration in environment variables.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = cfg.smtp_sender
    msg["To"] = cfg.recipient_email
    msg.attach(MIMEText(html_body, "html"))

    context = ssl.create_default_context()
    with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port) as server:
        server.starttls(context=context)
        server.login(cfg.smtp_user, cfg.smtp_password)
        server.sendmail(cfg.smtp_sender, [cfg.recipient_email], msg.as_string())


# lazy import to keep quick startup if mail isn't needed
import smtplib  # noqa: E402  # isort:skip


def init_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                dedupe_key TEXT PRIMARY KEY,
                source TEXT,
                job_title TEXT,
                company_name TEXT,
                location TEXT,
                description TEXT,
                posted_date TEXT,
                apply_link TEXT,
                remote INTEGER,
                relevance_score REAL,
                raw_payload TEXT,
                ingested_at TEXT
            )
            """
        )
        conn.commit()


def upsert_jobs(db_path: str, jobs: list[JobPost]) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        for job in jobs:
            conn.execute(
                """
                INSERT INTO jobs (
                    dedupe_key, source, job_title, company_name, location,
                    description, posted_date, apply_link, remote,
                    relevance_score, raw_payload, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dedupe_key) DO UPDATE SET
                    source=excluded.source,
                    description=excluded.description,
                    posted_date=excluded.posted_date,
                    apply_link=excluded.apply_link,
                    relevance_score=excluded.relevance_score,
                    raw_payload=excluded.raw_payload,
                    ingested_at=excluded.ingested_at
                """,
                (
                    job.dedupe_key(),
                    job.source,
                    job.job_title,
                    job.company_name,
                    job.location,
                    job.description,
                    job.posted_date.isoformat(),
                    job.apply_link,
                    1 if job.remote else 0,
                    job.relevance_score,
                    job.raw_payload,
                    now_iso,
                ),
            )
        conn.commit()


def write_snapshot_csv(jobs: list[JobPost], path: str = "latest_jobs_snapshot.csv") -> None:
    rows = [asdict(job) for job in jobs]
    if pd is not None:
        df = pd.DataFrame(rows)
        if not df.empty:
            df.to_csv(path, index=False)
        return

    import csv

    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def collect_jobs(cfg: Config) -> list[JobPost]:
    since_utc = datetime.now(timezone.utc) - timedelta(hours=24)

    sources: list[BaseSource] = [
        SerpAPISource(site_filter="linkedin.com/jobs"),
        SerpAPISource(site_filter="indeed.com"),
        SerpAPISource(site_filter="glassdoor.com"),
        RemotiveSource(),
    ]

    all_jobs: list[JobPost] = []
    for source in sources:
        logging.info("Fetching from source: %s", source.source_name)
        jobs = source.fetch(cfg, since_utc)
        logging.info("Source %s returned %s jobs", source.source_name, len(jobs))
        all_jobs.extend(jobs)

    deduped = deduplicate_jobs(all_jobs)
    ranked = rank_and_filter(deduped, cfg, since_utc)
    return ranked


def run_once(cfg: Config, dry_run: bool = False, write_sample: bool = True) -> None:
    logging.info("Job aggregation started")

    init_db(cfg.db_path)
    jobs = collect_jobs(cfg)
    upsert_jobs(cfg.db_path, jobs)

    top_jobs = jobs[: cfg.max_top_jobs]
    other_jobs = jobs[cfg.max_top_jobs :]

    now_ist = datetime.now(IST)
    subject = f"Daily Global Job Alerts – {now_ist.strftime('%Y-%m-%d')}"
    html_body = build_html_email(top_jobs, other_jobs, now_ist)

    if write_sample:
        with open("sample_email_output.html", "w", encoding="utf-8") as f:
            f.write(html_body)

    write_snapshot_csv(jobs, "latest_jobs_snapshot.csv")

    if dry_run:
        logging.info("Dry run enabled; email not sent.")
    else:
        send_email(cfg, subject, html_body)
        logging.info("Email sent to %s", cfg.recipient_email)

    logging.info("Job aggregation completed; %s jobs after filtering", len(jobs))


def scheduler_loop(cfg: Config) -> None:
    """Simple loop scheduler that triggers at 7:00 AM IST daily."""
    logging.info("Scheduler started; waiting for 07:00 IST daily trigger")
    while True:
        now_ist = datetime.now(IST)
        if now_ist.hour == 7 and now_ist.minute == 0:
            try:
                run_once(cfg, dry_run=False, write_sample=True)
            except Exception as exc:  # noqa: BLE001
                logging.exception("Scheduled run failed: %s", exc)
            time.sleep(61)
        else:
            time.sleep(20)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Daily job aggregation and notification system")
    p.add_argument("--dry-run", action="store_true", help="Run pipeline without sending email")
    p.add_argument(
        "--mode",
        default="once",
        choices=["once", "scheduler"],
        help="Run once or keep process alive and trigger at 7:00 AM IST daily",
    )
    return p


def main() -> None:
    cfg = Config()
    setup_logging(cfg.log_file)

    args = build_parser().parse_args()
    if args.mode == "once":
        run_once(cfg, dry_run=args.dry_run, write_sample=True)
    else:
        scheduler_loop(cfg)


if __name__ == "__main__":
    main()
