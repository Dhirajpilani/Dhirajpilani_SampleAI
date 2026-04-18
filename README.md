# Daily Global Job Aggregation & Notification System

A Python automation that collects latest jobs (last 24h), ranks them by relevance to R2R / Finance Transformation / BlackLine profiles, and emails a daily HTML digest at **7:00 AM IST**.

## ⚠️ Compliance & Policy Notes
- This implementation is **API-first**.
- It avoids direct scraping of authenticated pages.
- For LinkedIn / Indeed / Glassdoor discovery, it uses SerpAPI's Google Jobs endpoint when API key is provided.
- Always review each platform's Terms of Service and robots.txt before enabling any direct scraper.

---

## Features Implemented
- Multi-source fetch:
  - LinkedIn (via SerpAPI site-scoped discovery)
  - Indeed (via SerpAPI site-scoped discovery)
  - Glassdoor (via SerpAPI site-scoped discovery)
  - Additional global source: Remotive Jobs API
- Search criteria support:
  - Keywords:
    - R2R
    - Record to Report
    - Finance Transformation
    - BlackLine
    - Account Reconciliation
    - Senior Analyst
    - Manager
  - Mid/Senior heuristic filtering
  - Global coverage
  - Optional remote-only filter
  - Last 24h filtering
- Captured fields:
  - Job Title
  - Company Name
  - Location
  - Short Description
  - Posted Date
  - Apply Link
- Deduplication across sources
- Priority ranking:
  - BlackLine-heavy roles
  - R2R / Record to Report
  - Finance Transformation
  - High-quality companies boost
- Email output:
  - Subject: `Daily Global Job Alerts – [Current Date]`
  - Sections:
    - Top 10 Recommended Jobs
    - Other Relevant Opportunities
- SMTP delivery (Gmail / Outlook)
- Daily logging + retry-ready HTTP session
- Optional local persistence in SQLite (`jobs.db`)
- CSV export of latest run (`latest_jobs_snapshot.csv`)

---

## Project Files
- `job_alert_system.py` → Full automation script
- `.env.example` → Environment variable template
- `requirements.txt` → Python dependencies
- `cron_job.txt` → Crontab configuration sample
- `sample_email_output.html` → Sample email output

---

## Setup Instructions

## 1) Create Python environment and install dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Configure environment variables
Copy `.env.example` values into your shell/profile or use an env-loader tool.

Minimum required for email sending:
- `RECIPIENT_EMAIL`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASSWORD`
- `SMTP_SENDER`

Recommended API key:
- `SERPAPI_API_KEY`

> For Gmail, use an App Password (if 2FA enabled), not your normal account password.

## 3) Run once (dry run: no email)
```bash
python3 job_alert_system.py --mode once --dry-run
```

## 4) Run once (send email)
```bash
python3 job_alert_system.py --mode once
```

---

## Daily Scheduling at 7:00 AM IST

### Option A: Cron (Linux/Mac)
Use `cron_job.txt`.

Quick steps:
```bash
crontab -e
```
Add:
```cron
CRON_TZ=Asia/Kolkata
0 7 * * * /usr/bin/python3 /workspace/Dhirajpilani_SampleAI/job_alert_system.py --mode once >> /workspace/Dhirajpilani_SampleAI/cron.log 2>&1
```

### Option B: Keep process running (internal scheduler mode)
```bash
python3 job_alert_system.py --mode scheduler
```
The script checks time continuously and triggers at 07:00 IST.

---

## Data & Output
Each run generates:
- `job_alert.log` → execution logs
- `jobs.db` → SQLite job store (upserted by dedupe key)
- `latest_jobs_snapshot.csv` → tabular job output
- `sample_email_output.html` → rendered HTML body from current run

---

## Optional Enhancements (ready extension points)
- Telegram alerts (add bot API sender)
- WhatsApp notifications (Twilio/Meta API)
- AI ranking (LLM or embedding-based fit score)
- Direct source adapters where official APIs are available

---

## Notes on LinkedIn/Indeed/Glassdoor
These platforms can change markup and have strict anti-bot measures. To reduce policy and reliability risks:
- Prefer official APIs or approved aggregators.
- If direct scraping is introduced, implement robots.txt checks, lower request rates, and legal review.

