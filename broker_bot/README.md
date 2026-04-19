# Camelot OS — Broker Bot

AI-powered brokerage tools for **Camelot Realty Group**, supporting Broker of Record
**Eleni Palmeri** in NYC-area commercial and multifamily real estate transactions.

---

## What It Does

| Function | Description |
|----------|-------------|
| **LOI Generator** | Produces professional Letters of Intent (PDF) for CRE acquisitions |
| **Comp Analyzer** | Pulls NYC ACRIS sales comps and calculates per-SF, cap rate, GRM metrics |
| **Listing Analyzer** | Extracts and analyzes LoopNet/CoStar listing data |
| **Deal Memo Generator** | Full investment deal memo with IRR, NPV, and cash-on-cash analysis |
| **HubSpot Pipeline** | Node.js integration for "Camelot Brokerage" deal pipeline management |

---

## Architecture

```
broker_bot/
├── loi_generator.py         ← Letter of Intent PDF generator
├── comp_analyzer.py         ← NYC ACRIS comp puller (Socrata API)
├── listing_analyzer.py      ← LoopNet/CoStar listing parser
├── deal_memo_generator.py   ← Investment deal memo with IRR/NPV
├── hubspot_deals.js         ← Node.js HubSpot pipeline integration
├── skill_definition.md      ← Agent skill definition and capabilities
├── config.yaml              ← Non-secret configuration
├── .env.example             ← Environment variable template
└── README.md
```

---

## Quick Start

### 1. Install dependencies

```bash
# Python
pip install reportlab requests pyyaml python-dotenv numpy

# Node.js (hubspot_deals.js — no npm packages required)
# Node.js >= 16.x
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with real credentials
```

### 3. Generate an LOI

```python
from loi_generator import generate_loi

loi_path = generate_loi(
    property_address="123 Main St, Bronx, NY 10451",
    purchase_price=2_500_000,
    buyer_name="Camelot Property Management Services Corp",
    buyer_address="New York, NY",
    seller_name="XYZ Holdings LLC",
    deposit_amount=50_000,
    inspection_period_days=30,
    closing_days=60,
    output_dir="output/broker_bot/lois",
)
print(f"LOI: {loi_path}")
```

### 4. Pull ACRIS comps

```python
from comp_analyzer import pull_comps

comps = pull_comps(
    borough="BRONX",
    block="02345",
    lot="0010",
    radius_blocks=5,
    limit=25,
)
for comp in comps:
    print(f"{comp['address']}  ${comp['sale_price']:,}  {comp['price_per_sf']:.0f}/sf")
```

### 5. Generate a deal memo

```python
from deal_memo_generator import generate_deal_memo

memo_path = generate_deal_memo(
    property_address="123 Main St, Bronx, NY 10451",
    purchase_price=2_500_000,
    gross_income=320_000,
    vacancy_rate=0.05,
    operating_expenses=140_000,
    capex_reserve=15_000,
    financing={
        "loan_amount": 1_875_000,
        "interest_rate": 0.065,
        "amortization_years": 30,
        "loan_term_years": 10,
    },
    hold_period_years=7,
    exit_cap_rate=0.065,
    annual_rent_growth=0.03,
    output_dir="output/broker_bot/memos",
)
print(f"Memo: {memo_path}")
```

### 6. HubSpot pipeline (Node.js)

```bash
# Pipeline summary
node hubspot_deals.js summary

# List active listings
node hubspot_deals.js stage "Active Listing"

# Search by address
node hubspot_deals.js search "123 Main St"

# Create or update a deal
node hubspot_deals.js upsert '{"address":"123 Main St, Bronx, NY", "asking_price": 2500000}'
```

---

## NYC ACRIS Data

The Comp Analyzer uses NYC Open Data (Socrata) APIs:

| Dataset | URL | Usage |
|---------|-----|-------|
| ACRIS Sales | `data.cityofnewyork.us/resource/usep-8jbt.json` | Sales price, date, buyer/seller |
| ACRIS Legals | `data.cityofnewyork.us/resource/8h5j-fqxa.json` | Block/lot/BBL lookup |

No authentication required, but setting `NYC_OPEN_DATA_APP_TOKEN` significantly raises rate limits.

---

## HubSpot Pipeline: "Camelot Brokerage"

Deals are tracked in the `Camelot Brokerage` HubSpot pipeline.

| Stage | Description |
|-------|-------------|
| **Lead** | Property identified as acquisition target |
| **Underwriting** | Deal memo / financial analysis in progress |
| **LOI Submitted** | Letter of Intent sent to seller |
| **Under Contract** | Executed PSA, due diligence in progress |
| **Closed** | Transaction completed |

---

## Output Files

| File Type | Location | Naming Convention |
|-----------|----------|-------------------|
| LOI PDF | `output/broker_bot/lois/` | `loi_{address}_{date}.pdf` |
| Deal Memo PDF | `output/broker_bot/memos/` | `memo_{address}_{date}.pdf` |
| Comp Report PDF | `output/broker_bot/comps/` | `comps_{borough}_{bbl}_{date}.pdf` |
| Listing Analysis | `output/broker_bot/listings/` | `listing_{address}_{date}.json` |

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `HUBSPOT_ACCESS_TOKEN` | ✓ | HubSpot private app token |
| `NYC_OPEN_DATA_APP_TOKEN` | | Socrata token (rate limit avoidance) |
| `GOOGLE_PLACES_API_KEY` | | Address/listing enrichment |
| `SUPABASE_URL` | | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | | Supabase service key |
| `SMTP_HOST` | ✓ for email | SMTP server hostname |
| `BROKER_NAME` | | Defaults to `Eleni Palmeri` |
| `BROKER_EMAIL` | | Defaults to `eleni@camelot.nyc` |
| `FIRM_NAME` | | Defaults to `Camelot Realty Group` |
| `BROKER_BOT_OUTPUT_DIR` | | Output directory (default: `output/broker_bot`) |
| `LOG_LEVEL` | | `INFO` / `DEBUG` / `WARNING` |

---

## Dependencies

```
# Python
reportlab>=4.0.0
requests>=2.31.0
numpy>=1.24.0
pyyaml>=6.0
python-dotenv>=1.0.0
beautifulsoup4>=4.12.0
lxml>=5.0.0

# Node.js
# No npm packages — hubspot_deals.js uses only built-in https module
# Node.js >= 16.x required
```

---

## Broker of Record

**Eleni Palmeri** — Broker of Record, Camelot Realty Group
All documents generated by this bot are templates and require review and signature
by the Broker of Record before delivery.

---

*Camelot Property Management Services Corp — Camelot OS v1.0*
