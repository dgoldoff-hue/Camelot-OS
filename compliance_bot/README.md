# Compliance Bot
## Camelot Property Management Services Corp

AI-powered regulatory compliance monitor for Camelot's NYC and Tri-State portfolio. Scans HPD, DOB, LL97, rent stabilization, and permit status across all managed buildings. Dispatches structured alert digests to the operations team.

---

## Features

| Module | What it checks |
|--------|---------------|
| `hpd_violations.py` | Open HPD violations (Class A/B/C), deadlines, heat season alerts |
| `dob_violations.py` | DOB violations, ECB, stop work orders, expired permits |
| `ll97_monitor.py` | Local Law 97 carbon emissions, penalty exposure, Energy Star scores |
| `rent_stab_checker.py` | HCR/DHCR registration status, unregistered RS buildings |
| `alerts.py` | Portfolio scan orchestration, digest generation, email dispatch |

---

## Quick Start

```bash
# 1. Install dependencies
pip install requests

# 2. Configure environment
cp .env.example .env
# Edit .env with your SMTP and API credentials

# 3. Create portfolio.json (see format below)

# 4. Run full scan (dry run first)
python main.py --dry-run

# 5. Run with email alerts
python main.py

# 6. Scan a single building
python main.py --building 2025010012
```

---

## Portfolio JSON Format

```json
[
    {
        "building_id": "CAM-001",
        "address": "123 Main Street, Bronx, NY 10452",
        "bbl": "2025010012",
        "bin": "2000001",
        "gross_sq_ft": 25000,
        "asset_type": "multifamily",
        "electricity_kwh": 150000,
        "natural_gas_kbtu": 1000000
    }
]
```

**Required fields:** `address`  
**Recommended:** `bbl` (for most precise lookups), `bin` (for DOB permits), `gross_sq_ft` + energy data (for LL97)

---

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `NYC_OPEN_DATA_APP_TOKEN` | NYC Open Data app token | Recommended |
| `SMTP_HOST` | SMTP server hostname | Yes (for email) |
| `SMTP_PORT` | SMTP port (default: 587) | Yes (for email) |
| `SMTP_USER` | SMTP username | Yes (for email) |
| `SMTP_PASSWORD` | SMTP password/app password | Yes (for email) |
| `SUPABASE_URL` | Supabase project URL | Optional |
| `SUPABASE_SERVICE_KEY` | Supabase service key | Optional |
| `PORTFOLIO_JSON` | Path to portfolio file | Optional |
| `LOG_LEVEL` | Logging verbosity (INFO/DEBUG) | Optional |

---

## Alert Severity Levels

| Level | Trigger | Action Required |
|-------|---------|----------------|
| **CRITICAL** | Class C HPD violations, Stop Work Orders, ECB violations, LL97 non-compliance | Immediate — same business day |
| **WARNING** | Class B HPD violations, approaching deadlines, marginal LL97, expired permits | Within 48 hours |
| **INFO** | Class A violations, new permit filings, RS monitoring updates | Within 1 week |

---

## Scheduled Runs (Cron)

```cron
# Daily full scan at 7am
0 7 * * * cd /opt/camelot/compliance_bot && python main.py >> logs/cron.log 2>&1

# Critical-only check every 4 hours
0 */4 * * * cd /opt/camelot/compliance_bot && python main.py --skip-rs --skip-ll97 >> logs/cron.log 2>&1
```

---

## Alert Recipients

- **David Goldoff** — `dgoldoff@camelot.nyc`
- **C. Harkien** — `charkien@camelot.nyc`

---

## Data Sources

- NYC Open Data / HPD: `data.cityofnewyork.us`
- NYC DOB BIS / DOB NOW: `data.cityofnewyork.us`
- NYC Benchmarking (LL84/97): `data.cityofnewyork.us`
- HCR/DHCR: NYC HCR Open Data + `apps.hcr.ny.gov`
- MapPLUTO: NYC Planning Open Data
