# Index Bot
## Camelot Property Management Services Corp

AI-powered Google Drive file indexer and organizer. Auto-classifies, renames, and moves documents using MDS building codes. Integrates with Make.com for fully automated document routing.

**Indexing Lead:** Sam Lodge | **Pilot Building:** 552

---

## MDS Naming Convention

```
{MDS_CODE}_{DOC_TYPE}_{YYYY-MM-DD}_{VERSION}.{ext}

Example: 552_LEASE_2026-04-01_v1.pdf
```

---

## Quick Start

```bash
# 1. Install dependencies
pip install google-api-python-client google-auth fastapi uvicorn pydantic

# 2. Configure environment
cp .env.example .env
# Set GOOGLE_SERVICE_ACCOUNT_JSON, SHEETS_INDEX_SPREADSHEET_ID, MAKE_WEBHOOK_SECRET

# 3. Share /Camelot/ Drive folder with service account email (Editor access)

# 4. Test classification
python main.py classify "john_invoice_march_2026.pdf" --mds-code 552

# 5. Scan for unindexed files (dry run)
python main.py scan <google_drive_folder_id>

# 6. Process files (dry run first)
python main.py process <incoming_folder_id> 552 --dry-run
python main.py process <incoming_folder_id> 552

# 7. Generate index report
python main.py index 552

# 8. Start API server (for Make.com integration)
python main.py serve --port 8002
```

---

## Make.com Integration

1. Import `make_scenario.yaml` into Make.com
2. Replace all `[PLACEHOLDER]` values in the scenario
3. Set `MAKE_WEBHOOK_SECRET` in both Make.com and your `.env`
4. Start the API server: `python main.py serve`
5. Activate the Make.com scenario
6. Drop a file into `/Camelot/Incoming/` — it auto-classifies, renames, and moves

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/classify` | Classify filename → MDS name |
| `POST` | `/rename` | Rename + move a Drive file |
| `POST` | `/make/incoming` | Make.com webhook handler |
| `GET` | `/index/{mds_code}` | Building file index |
| `GET` | `/buildings` | List all MDS codes |
| `POST` | `/classify-batch` | Classify multiple filenames |

---

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Path to service account JSON | Yes |
| `SHEETS_INDEX_SPREADSHEET_ID` | Google Sheets spreadsheet ID for index log | Yes |
| `MAKE_WEBHOOK_SECRET` | Shared secret for Make.com webhook | Recommended |
| `API_HOST` / `API_PORT` | API server bind address/port | Optional |
| `CAMELOT_DRIVE_ROOT` | Root folder name in Drive (default: Camelot) | Optional |

---

## Document Type Codes

| Code | Document Type |
|------|--------------|
| LEASE | Lease agreement |
| INVOICE | Vendor invoice, utility bill |
| PERMIT | DOB/work permit |
| VIOLATION | HPD/DOB/ECB violation notice |
| REPORT | Inspection or engineering report |
| FINANCIAL | P&L, rent roll, bank statement |
| CORRESPONDENCE | Letters, legal notices, emails |
| INSURANCE | COI, policy documents |
| CONTRACT | Vendor/service contracts |
| CO | Certificate of Occupancy |
