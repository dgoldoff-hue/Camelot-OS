# Camelot OS — Report Bot

Automated report generation for Camelot Property Management Services Corp.
Produces owner statements, weekly KPI dashboards, and quarterly investor updates
as professional PDFs delivered by email on a scheduled basis.

---

## Reports Generated

| Report | Trigger | Output |
|--------|---------|--------|
| **Owner Statements** | 1st of month at 06:00 | Per-property PDF income statements emailed to owners |
| **KPI Dashboard** | Every Monday at 08:00 | PDF + Markdown with ▲▼ trend indicators vs. prior week |
| **Investor Update** | 1st of Jan / Apr / Jul / Oct at 07:00 | Quarterly investor-grade PDF with full portfolio financials |

---

## Architecture

```
report_bot/
├── main.py               ← CLI + FastAPI HTTP entry point (port 8003)
├── scheduler.py          ← Cron scheduler daemon (schedule library)
├── owner_statement.py    ← Monthly owner income statement generator
├── kpi_dashboard.py      ← Weekly KPI dashboard (Markdown + PDF)
├── investor_update.py    ← Quarterly investor update PDF
├── config.yaml           ← Non-secret configuration
├── .env.example          ← Environment variable template
└── README.md
```

**Data sources:**
- **Supabase** — `units`, `rent_payments`, `work_orders`, `quarterly_financials`, `capex_projects`, `kpi_snapshots`
- **HubSpot CRM** — Scout leads, pipeline value (Camelot Roll-Up + Camelot Brokerage pipelines)
- **NYC Open Data (Socrata)** — HPD + DOB open violations
- **MDS** — Property-level financial data (via CSV export cache)

**PDF generation:** reportlab (pure Python, no LaTeX dependency)

---

## Quick Start

### 1. Install dependencies

```bash
pip install reportlab requests schedule fastapi uvicorn pyyaml python-dotenv
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 3. Run a report immediately

```bash
# Generate all owner statements for this month
python main.py owner-statements

# Generate weekly KPI dashboard
python main.py kpi-dashboard

# Generate quarterly investor update (current quarter)
python main.py investor-update

# Generate for a specific quarter
python main.py investor-update --year 2025 --quarter 3
```

### 4. Start the scheduler daemon

```bash
python main.py scheduler
# or via the CLI helper
python scheduler.py --daemon
```

### 5. Start the HTTP API server

```bash
python main.py serve
# Server starts on port 8003 by default
```

---

## HTTP API

When running in `serve` mode, the following endpoints are available:

### `GET /reports/status`
Health check.

```json
{"status": "ok", "service": "Camelot Report Bot", "timestamp": "2025-04-01T06:00:00"}
```

### `POST /reports/owner-statements`
Generate owner statements.

```json
{"year": 2025, "month": 3}
```

Response:
```json
{
  "status": "success",
  "count": 12,
  "output_dir": "output/reports/owner_statements/2025-03",
  "statements": [{"property": "552 W 150th St", "pdf": "output/reports/...pdf"}]
}
```

### `POST /reports/kpi-dashboard`
Generate KPI dashboard.

```json
{"persist": true}
```

Response:
```json
{"status": "success", "markdown_path": "...", "pdf_path": "..."}
```

### `POST /reports/investor-update`
Generate investor update PDF.

```json
{"year": 2025, "quarter": 2}
```

Response:
```json
{"status": "success", "pdf_path": "output/reports/investor_updates/investor_update_2025_Q2.pdf"}
```

---

## Supabase Schema

The Report Bot expects the following tables in your Supabase project:

```sql
-- Unit occupancy
CREATE TABLE units (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  building_id  TEXT NOT NULL,
  unit_number  TEXT NOT NULL,
  status       TEXT NOT NULL CHECK (status IN ('occupied', 'vacant', 'down'))
);

-- Rent payments
CREATE TABLE rent_payments (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  unit_id       UUID REFERENCES units(id),
  period_year   INT NOT NULL,
  period_month  INT NOT NULL,
  amount_due    NUMERIC(12,2) NOT NULL,
  amount_paid   NUMERIC(12,2) DEFAULT 0,
  paid_date     DATE
);

-- Work orders
CREATE TABLE work_orders (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  status      TEXT NOT NULL CHECK (status IN ('open', 'in_progress', 'closed', 'cancelled')),
  created_at  TIMESTAMPTZ DEFAULT NOW(),
  closed_at   TIMESTAMPTZ
);

-- KPI snapshots (for week-over-week comparison)
CREATE TABLE kpi_snapshots (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  metric_name   TEXT NOT NULL,
  value         NUMERIC NOT NULL,
  snapshot_date DATE NOT NULL,
  UNIQUE (metric_name, snapshot_date)
);

-- Quarterly financials (view or materialized from MDS export)
CREATE TABLE quarterly_financials (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_name        TEXT NOT NULL,
  address              TEXT,
  units                INT,
  period_year          INT NOT NULL,
  period_quarter       INT NOT NULL,
  gross_revenue        NUMERIC(14,2),
  operating_expenses   NUMERIC(14,2),
  noi                  NUMERIC(14,2),
  occupancy_pct        NUMERIC(5,2),
  capex                NUMERIC(14,2) DEFAULT 0
);

-- Capital improvements
CREATE TABLE capex_projects (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  property_name    TEXT NOT NULL,
  description      TEXT,
  amount           NUMERIC(14,2),
  completion_date  DATE,
  status           TEXT CHECK (status IN ('planned', 'in_progress', 'completed'))
);
```

---

## Scheduler Cron Reference

The scheduler uses the [`schedule`](https://schedule.readthedocs.io) library and
runs as a blocking daemon process. It fires checks every 30 seconds.

| Job | Schedule | Guard |
|-----|----------|-------|
| `schedule_monthly_statements` | Daily at `MONTHLY_STATEMENTS_TIME` | Only executes on day == 1 |
| `schedule_weekly_kpi` | Every Monday at `WEEKLY_KPI_TIME` | None (runs every Monday) |
| `schedule_quarterly_update` | Daily at `QUARTERLY_UPDATE_TIME` | Only executes on day == 1 and month in {1, 4, 7, 10} |

To run as a system service, create a systemd unit:

```ini
[Unit]
Description=Camelot Report Bot Scheduler
After=network.target

[Service]
Type=simple
User=camelot
WorkingDirectory=/opt/camelot_os/report_bot
EnvironmentFile=/opt/camelot_os/report_bot/.env
ExecStart=/usr/bin/python3 scheduler.py --daemon
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SUPABASE_URL` | ✓ | — | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | ✓ | — | Supabase service role key |
| `HUBSPOT_ACCESS_TOKEN` | ✓ | — | HubSpot private app token |
| `SMTP_HOST` | ✓ for email | — | SMTP server hostname |
| `SMTP_PORT` | | `587` | SMTP port |
| `SMTP_USER` | | — | SMTP username |
| `SMTP_PASSWORD` | | — | SMTP password |
| `NYC_OPEN_DATA_APP_TOKEN` | | — | Socrata app token (rate limit avoidance) |
| `PORTFOLIO_BBLS` | | — | Comma-separated BBLs for violation queries |
| `REPORT_OUTPUT_DIR` | | `output/reports` | Base directory for generated files |
| `REPORT_ALERT_EMAIL` | | `dgoldoff@camelot.nyc` | Primary report recipient |
| `REPORT_EMAIL_DELIVERY` | | `true` | Set `false` to disable email sends |
| `REPORT_BOT_PORT` | | `8003` | HTTP API port |
| `LOG_LEVEL` | | `INFO` | Python log level |
| `LOG_FILE` | | `logs/report_bot.log` | Log file path |

---

## Dependencies

```
reportlab>=4.0.0
requests>=2.31.0
schedule>=1.2.0
fastapi>=0.110.0
uvicorn>=0.29.0
pyyaml>=6.0
python-dotenv>=1.0.0
```

---

*Camelot Property Management Services Corp — Camelot OS v1.0*
