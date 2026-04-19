# Camelot OS — Deal Bot

AI-powered acquisition pipeline management for the **Camelot Roll-Up** program.
Identifies, researches, and engages independent property management operators as
acquisition prospects across NYC, Westchester, CT, NJ, and FL.

---

## What It Does

| Function | Description |
|----------|-------------|
| **Prospect Mapping** | Research target companies using NYC HPD, HCR, ACRIS + Google Places |
| **Outreach Generation** | Personalized emails calibrated to angle (succession/growth/systems-upgrade/tired-operator) |
| **Email Sequences** | Automated 5-email drip sequences: Day 1 → 3 → 7 → 14 → 30 |
| **HubSpot Pipeline** | Upsert prospects as deals, log outreach activities, manage stage progression |
| **Battlecards** | One-page pre-meeting prep: snapshot, pain points, value props, discovery questions |

---

## Architecture

```
deal_bot/
├── main.py                  ← CLI + FastAPI HTTP entry point (port 8004)
├── prospect_mapper.py       ← Company research + ProspectProfile builder
├── outreach_generator.py    ← Personalized email generation (12 angle×structure variants)
├── email_sequences.py       ← 5-email drip sequence builder, SMTP sender, Supabase persistence
├── hubspot_pipeline.js      ← Node.js HubSpot CRM integration
├── battlecard_generator.py  ← PDF + Markdown battlecard generator
├── config.yaml              ← Non-secret configuration
├── .env.example             ← Environment variable template
└── README.md
```

**Data sources:**
- **NYC HPD Registrations** — building ownership, unit counts, portfolio mapping
- **HCR Rent Stabilization Registry** — RS portfolio identification
- **Google Places API** — company address, phone, website enrichment
- **Prospeo API** — decision-maker email discovery
- **HubSpot CRM** — deal pipeline, contact management, outreach logging
- **Supabase** — email sequence persistence and status tracking

---

## Quick Start

### 1. Install dependencies

```bash
# Python
pip install requests reportlab fastapi uvicorn pyyaml python-dotenv

# Node.js (for HubSpot pipeline)
# No npm packages — uses only built-in https module
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 3. Research a prospect

```bash
# By company name
python main.py prospect --company "Acme Property Management" --city "Bronx"

# By owner name
python main.py prospect --owner "John Smith" --city "Brooklyn"

# Batch research from JSON file
python main.py prospect --batch targets.json
```

### 4. Generate outreach email

```bash
python main.py outreach --profile output/deal_bot/profiles/prospect_Acme.json

# Override angle and structure
python main.py outreach --profile ... --angle succession --structure equity-sale

# Generate all angle variants for A/B testing
python main.py outreach --profile ... --all-angles
```

### 5. Create a drip sequence

```bash
# Preview without sending
python main.py sequence --profile ... --email owner@acme.com --preview

# Create and enqueue (persists to Supabase)
python main.py sequence --profile ... --email owner@acme.com --deal-id 12345678
```

### 6. Send pending emails (run daily by cron)

```bash
python main.py sequences
# or via cron: 0 9 * * * python /opt/camelot_os/deal_bot/main.py sequences
```

### 7. Generate a battlecard

```bash
python main.py battlecard --profile output/deal_bot/profiles/prospect_Acme.json

# Preview as Markdown
python main.py battlecard --profile ... --preview
```

### 8. HubSpot pipeline operations

```bash
# Pipeline summary
python main.py hubspot summary

# List deals in a stage
python main.py hubspot stage "Meeting Scheduled"

# Search by company name
python main.py hubspot search "Acme"

# Get deal details
python main.py hubspot get 12345678

# Upsert prospect to HubSpot from JSON
python main.py hubspot upsert --profile output/deal_bot/profiles/prospect_Acme.json
```

---

## HTTP API

Start the server: `python main.py serve`

### `GET /deal/status`
Health check.

### `POST /deal/prospect`
Research a prospect.
```json
{"company_name": "Acme Property Management", "city": "Bronx"}
```

### `POST /deal/outreach`
Generate outreach email.
```json
{"profile_path": "output/deal_bot/profiles/prospect_Acme.json", "angle": "succession"}
```

### `POST /deal/sequence`
Create email sequence.
```json
{"profile_path": "...", "prospect_email": "owner@acme.com", "hubspot_deal_id": "12345"}
```

### `POST /deal/sequences/run`
Send all pending emails due today.

### `POST /deal/battlecard`
Generate battlecard.
```json
{"profile_path": "...", "formats": ["pdf", "md"]}
```

---

## HubSpot Pipeline: "Camelot Roll-Up"

| Stage | Description |
|-------|-------------|
| **Identified** | Prospect researched, profile built |
| **Contacted** | First outreach sent (auto-advanced when email logged) |
| **Responded** | Prospect replied positively or neutrally |
| **Meeting Scheduled** | Call or in-person meeting confirmed |
| **Term Sheet** | Term sheet issued to prospect |
| **Closed** | Transaction closed |

**Custom deal properties** set by `hubspot_pipeline.js`:
- `camelot_prospect_score` — Fit score (0–100)
- `camelot_outreach_angle` — succession / growth / systems-upgrade / tired-operator
- `camelot_deal_structure` — equity-sale / roll-up / powered-by
- `camelot_estimated_units` — Portfolio size estimate

---

## Supabase Schema

```sql
-- Email sequences
CREATE TABLE email_sequences (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sequence_id     TEXT NOT NULL,
  step_number     INT NOT NULL,
  day_offset      INT NOT NULL,
  scheduled_date  DATE NOT NULL,
  subject         TEXT NOT NULL,
  body            TEXT NOT NULL,
  angle           TEXT,
  structure       TEXT,
  prospect_email  TEXT NOT NULL,
  prospect_name   TEXT,
  company_name    TEXT,
  hubspot_deal_id TEXT,
  status          TEXT DEFAULT 'pending',
  sent_at         TIMESTAMPTZ,
  opened_at       TIMESTAMPTZ,
  replied_at      TIMESTAMPTZ,
  UNIQUE (sequence_id, step_number)
);
```

---

## Outreach Angles & When to Use Each

| Angle | Signals | Template Focus |
|-------|---------|----------------|
| `succession` | Tenure ≥ 15 years | Legacy, liquidity, staff retention |
| `growth` | 100+ units, < 10 years tenure | Capital access, scaling systems |
| `systems-upgrade` | > 10 open violations OR no website | Compliance automation, tech |
| `tired-operator` | Default fallback | Reducing workload, partial exit |

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `HUBSPOT_ACCESS_TOKEN` | ✓ | — | HubSpot private app token |
| `SUPABASE_URL` | ✓ | — | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | ✓ | — | Supabase service role key |
| `PROSPEO_API_KEY` | | `pk_6f97...` | Prospeo email finder API key |
| `GOOGLE_PLACES_API_KEY` | | — | Google Places for company enrichment |
| `NYC_OPEN_DATA_APP_TOKEN` | | — | Socrata token (avoids rate limits) |
| `SMTP_HOST` | ✓ for email | — | SMTP server |
| `DEAL_BOT_SENDER_EMAIL` | | `dgoldoff@camelot.nyc` | Outreach from address |
| `DEAL_BOT_SENDER_NAME` | | `David Goldoff` | Outreach sender name |
| `DEAL_BOT_OUTPUT_DIR` | | `output/deal_bot` | Base output directory |
| `DEAL_BOT_PORT` | | `8004` | HTTP API port |
| `LOG_LEVEL` | | `INFO` | Python log level |

---

## Dependencies

```
# Python
reportlab>=4.0.0
requests>=2.31.0
fastapi>=0.110.0
uvicorn>=0.29.0
pyyaml>=6.0
python-dotenv>=1.0.0

# Node.js (built-in only — no npm packages required)
# Node.js >= 16.x
```

---

*Camelot Property Management Services Corp — Camelot OS v1.0*
