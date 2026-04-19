"""
make_webhook.py — Make.com (Integromat) Webhook Handler
Camelot Property Management Services Corp / Index Bot

Processes incoming Make.com webhooks triggered when a new file
is added to /Camelot/Incoming/ in Google Drive.

Webhook flow:
    New file in Incoming/ → Make.com → POST /make/incoming → Index Bot
    → classify → rename → move → log to Sheets

Author: Camelot OS
"""

import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Webhook auth secret — Make.com sends this in a header for verification
WEBHOOK_SECRET = os.getenv("MAKE_WEBHOOK_SECRET", "")


# ---------------------------------------------------------------------------
# Webhook payload schema
# ---------------------------------------------------------------------------

# Expected Make.com webhook payload structure:
# {
#     "file_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms",
#     "file_name": "john_invoice_march.pdf",
#     "folder_id": "incoming_folder_id",
#     "folder_name": "Incoming",
#     "created_time": "2026-04-01T12:00:00Z",
#     "modified_time": "2026-04-01T12:01:00Z",
#     "mime_type": "application/pdf",
#     "size": 245000,
#     "web_view_link": "https://drive.google.com/file/d/...",
#     "created_by": "sam.lodge@camelot.nyc",
#     "content_hint": "Invoice from ABC Plumbing, March 2026",   # optional
#     "default_mds_code": "552",                                  # optional
#     "dry_run": false                                             # optional
# }


def receive_make_webhook(
    payload: dict,
    secret_header: Optional[str] = None,
    dry_run_override: bool = False,
) -> dict:
    """
    Process an incoming Make.com webhook when a new file is added to Drive.

    Pipeline:
        1. Validate webhook secret
        2. Extract file metadata from payload
        3. Classify document type → determine MDS code
        4. Generate new MDS-compliant filename
        5. Rename file in Drive
        6. Move file to correct folder
        7. Log action to Google Sheets

    Args:
        payload:          Parsed JSON body from Make.com webhook.
        secret_header:    Value of X-Webhook-Secret header (for validation).
        dry_run_override: Force dry run regardless of payload flag.

    Returns:
        Processing result dict with: success, file_id, original_name,
        new_name, mds_code, doc_type, moved_to_folder, logged, errors.
    """
    start_time = datetime.utcnow().isoformat() + "Z"

    # Validate secret
    if WEBHOOK_SECRET and secret_header != WEBHOOK_SECRET:
        logger.warning("Make.com webhook received with invalid secret — rejected")
        return {
            "success": False,
            "error": "Invalid webhook secret",
            "timestamp": start_time,
        }

    # Extract required fields
    file_id = payload.get("file_id")
    file_name = payload.get("file_name", "")
    content_hint = payload.get("content_hint")
    default_mds_code = payload.get("default_mds_code")
    dry_run = dry_run_override or payload.get("dry_run", False)

    if not file_id:
        return {
            "success": False,
            "error": "Missing required field: file_id",
            "timestamp": start_time,
        }

    logger.info(
        f"Make.com webhook: file_id={file_id}, name='{file_name}', "
        f"dry_run={dry_run}, default_mds={default_mds_code}"
    )

    result = {
        "success": False,
        "file_id": file_id,
        "original_name": file_name,
        "new_name": None,
        "mds_code": None,
        "doc_type": None,
        "moved_to_folder": None,
        "logged": False,
        "errors": [],
        "timestamp": start_time,
        "dry_run": dry_run,
        "payload_echo": {
            "folder_id": payload.get("folder_id"),
            "created_by": payload.get("created_by"),
            "size": payload.get("size"),
            "mime_type": payload.get("mime_type"),
        },
    }

    # Step 1: Classify and generate filename
    try:
        from index_bot.mds_mapper import suggest_filename_from_original
        suggestion = suggest_filename_from_original(
            file_name,
            content_hint=content_hint,
            default_mds_code=default_mds_code,
        )
        result["mds_code"] = suggestion["mds_code"]
        result["doc_type"] = suggestion["doc_type"]
        result["new_name"] = suggestion["suggested_filename"]
        result["classification_confidence"] = suggestion["confidence"]
        logger.info(
            f"Classified '{file_name}' → MDS={suggestion['mds_code']}, "
            f"type={suggestion['doc_type']}, new_name='{suggestion['suggested_filename']}'"
        )
    except Exception as e:
        err = f"Classification failed: {e}"
        result["errors"].append(err)
        logger.error(err)
        return result

    if dry_run:
        result["success"] = True
        result["dry_run_actions"] = [
            f"Would rename: '{file_name}' → '{result['new_name']}'",
            f"Would move to: /Camelot/{result['mds_code']}/{result['doc_type']}/",
            "Would log to Google Sheets index",
        ]
        logger.info(f"[DRY RUN] Completed for file: {file_name}")
        return result

    # Step 2: Rename in Drive
    try:
        from index_bot.drive_organizer import rename_file
        rename_file(file_id, result["new_name"])
        logger.info(f"Renamed '{file_name}' → '{result['new_name']}'")
    except Exception as e:
        err = f"Rename failed: {e}"
        result["errors"].append(err)
        logger.error(err)
        # Continue to attempt move even if rename failed

    # Step 3: Move to correct folder
    if result["mds_code"] and result["doc_type"]:
        try:
            from index_bot.drive_organizer import move_to_correct_folder, move_to_unresolved
            moved = move_to_correct_folder(file_id, result["mds_code"], result["doc_type"])
            result["moved_to_folder"] = f"/Camelot/{result['mds_code']}/{result['doc_type']}/"
            logger.info(f"Moved file to {result['moved_to_folder']}")
        except Exception as e:
            err = f"Move failed: {e}"
            result["errors"].append(err)
            logger.error(err)
    else:
        try:
            from index_bot.drive_organizer import move_to_unresolved
            move_to_unresolved(file_id, reason=f"No MDS code for: {file_name}")
            result["moved_to_folder"] = "/Camelot/Incoming/UNRESOLVED/"
            logger.warning(f"Moved '{file_name}' to UNRESOLVED — no MDS code")
        except Exception as e:
            result["errors"].append(f"UNRESOLVED move failed: {e}")

    # Step 4: Log to Google Sheets
    try:
        sheets_result = _log_to_sheets(
            file_id=file_id,
            original_name=file_name,
            new_name=result["new_name"],
            mds_code=result.get("mds_code"),
            doc_type=result.get("doc_type"),
            moved_to=result.get("moved_to_folder"),
            triggered_by=payload.get("created_by", "make.com"),
            timestamp=start_time,
        )
        result["logged"] = sheets_result
    except Exception as e:
        result["errors"].append(f"Sheets logging failed: {e}")
        logger.warning(f"Failed to log to Google Sheets: {e}")

    result["success"] = len(result["errors"]) == 0
    logger.info(
        f"Make webhook processed: success={result['success']}, "
        f"errors={len(result['errors'])}"
    )
    return result


# ---------------------------------------------------------------------------
# Google Sheets logging
# ---------------------------------------------------------------------------

SHEETS_INDEX_NAME = os.getenv("SHEETS_INDEX_NAME", "Camelot Drive Index")
SHEETS_WORKSHEET_NAME = "Index Log"


def _log_to_sheets(
    file_id: str,
    original_name: str,
    new_name: Optional[str],
    mds_code: Optional[str],
    doc_type: Optional[str],
    moved_to: Optional[str],
    triggered_by: str,
    timestamp: str,
) -> bool:
    """
    Append a row to the Google Sheets master index log.

    Sheet columns: Timestamp, File ID, Original Name, New Name,
    MDS Code, Doc Type, Moved To, Triggered By
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        creds_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
        if not os.path.exists(creds_path):
            logger.warning(f"Service account JSON not found at {creds_path} — skipping Sheets log")
            return False

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = service_account.Credentials.from_service_account_file(
            creds_path, scopes=scopes
        )
        sheets_service = build("sheets", "v4", credentials=credentials, cache_discovery=False)

        # Find the sheet by name
        spreadsheet_id = os.getenv("SHEETS_INDEX_SPREADSHEET_ID", "")
        if not spreadsheet_id:
            logger.warning("SHEETS_INDEX_SPREADSHEET_ID not set — skipping Sheets log")
            return False

        row = [
            timestamp,
            file_id,
            original_name,
            new_name or "",
            mds_code or "UNKNOWN",
            doc_type or "UNKNOWN",
            moved_to or "",
            triggered_by,
        ]

        body = {"values": [row]}
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{SHEETS_WORKSHEET_NAME}!A:H",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()

        logger.info(f"Logged to Google Sheets: {new_name} → {moved_to}")
        return True

    except ImportError:
        logger.warning("Google Sheets API not available — skipping log")
        return False
    except Exception as e:
        logger.error(f"Sheets logging error: {e}")
        return False


# ---------------------------------------------------------------------------
# Webhook response helpers
# ---------------------------------------------------------------------------

def make_success_response(result: dict) -> dict:
    """Format a Make.com-compatible success response."""
    return {
        "status": "success" if result["success"] else "partial",
        "file_id": result.get("file_id"),
        "new_filename": result.get("new_name"),
        "mds_code": result.get("mds_code"),
        "doc_type": result.get("doc_type"),
        "destination_folder": result.get("moved_to_folder"),
        "logged": result.get("logged", False),
        "errors": result.get("errors", []),
        "dry_run": result.get("dry_run", False),
    }


# ---------------------------------------------------------------------------
# CLI (simulate webhook)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import sys
    logging.basicConfig(level=logging.INFO)

    # Simulate a Make.com webhook payload
    test_payload = {
        "file_id": "test_file_id_12345",
        "file_name": "john_invoice_march_2026.pdf",
        "folder_id": "incoming_folder_id",
        "folder_name": "Incoming",
        "created_by": "sam.lodge@camelot.nyc",
        "content_hint": "Invoice from ABC Plumbing for Building 552",
        "default_mds_code": "552",
        "dry_run": True,  # Safe for testing
    }

    result = receive_make_webhook(test_payload)
    print(json.dumps(make_success_response(result), indent=2))
