"""
drive_organizer.py — Google Drive Organizer
Camelot Property Management Services Corp / Index Bot

Organizes Camelot's Google Drive using MDS naming conventions.
Finds unindexed files, renames them, and moves them to structured folders.

Required env var:
    GOOGLE_SERVICE_ACCOUNT_JSON — path to service account credentials JSON file

Author: Camelot OS
"""

import csv
import io
import json
import logging
import os
import re
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
CAMELOT_ROOT_FOLDER_NAME = os.getenv("CAMELOT_DRIVE_ROOT", "Camelot")
INCOMING_FOLDER_NAME = "Incoming"
UNRESOLVED_FOLDER_NAME = "UNRESOLVED"
INDEX_FOLDER_NAME = "_Index"

# MDS filename pattern: 552_LEASE_2026-04-01_v1.pdf
MDS_FILENAME_PATTERN = re.compile(
    r"^[A-Z0-9\-]+_(?:LEASE|INVOICE|PERMIT|VIOLATION|REPORT|FINANCIAL|"
    r"CORRESPONDENCE|INSURANCE|CONTRACT|CO)_\d{4}-\d{2}-\d{2}_v\d+\.\w+$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Drive service builder
# ---------------------------------------------------------------------------

def _build_drive_service():
    """
    Build an authenticated Google Drive API service using a service account.

    Returns:
        Google API Resource object for Drive v3.

    Raises:
        FileNotFoundError: If service account JSON file is not found.
        google.auth.exceptions.GoogleAuthError: On auth failure.
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        raise ImportError(
            "Google API client not installed. Run: "
            "pip install google-api-python-client google-auth google-auth-httplib2"
        )

    creds_path = GOOGLE_SERVICE_ACCOUNT_JSON
    if not os.path.exists(creds_path):
        raise FileNotFoundError(
            f"Service account JSON not found at: {creds_path}. "
            f"Set GOOGLE_SERVICE_ACCOUNT_JSON env var to the correct path."
        )

    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]

    credentials = service_account.Credentials.from_service_account_file(
        creds_path, scopes=scopes
    )
    service = build("drive", "v3", credentials=credentials, cache_discovery=False)
    logger.debug("Google Drive service authenticated successfully")
    return service


_drive_service = None


def get_drive_service():
    """Get or initialize the Drive service (lazy singleton)."""
    global _drive_service
    if _drive_service is None:
        _drive_service = _build_drive_service()
    return _drive_service


# ---------------------------------------------------------------------------
# Folder helpers
# ---------------------------------------------------------------------------

def find_folder(name: str, parent_id: Optional[str] = None) -> Optional[str]:
    """
    Find a folder by name, optionally within a parent folder.

    Args:
        name:       Folder name to search for
        parent_id:  Parent folder ID (None = search all)

    Returns:
        Folder ID string or None if not found.
    """
    service = get_drive_service()
    query = f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"

    try:
        result = service.files().list(
            q=query, spaces="drive", fields="files(id, name)", pageSize=5
        ).execute()
        files = result.get("files", [])
        if files:
            return files[0]["id"]
        return None
    except Exception as e:
        logger.error(f"Error finding folder '{name}': {e}")
        return None


def get_or_create_folder(name: str, parent_id: str) -> str:
    """
    Get a folder by name within a parent, creating it if it doesn't exist.

    Args:
        name:       Folder name
        parent_id:  Parent folder Drive ID

    Returns:
        Folder ID string.
    """
    existing = find_folder(name, parent_id)
    if existing:
        return existing

    service = get_drive_service()
    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    try:
        folder = service.files().create(
            body=metadata, fields="id"
        ).execute()
        folder_id = folder["id"]
        logger.info(f"Created Drive folder: '{name}' (ID: {folder_id}) under parent {parent_id}")
        return folder_id
    except Exception as e:
        logger.error(f"Failed to create folder '{name}': {e}")
        raise


def get_camelot_root_id() -> str:
    """Find the /Camelot root folder ID."""
    root_id = find_folder(CAMELOT_ROOT_FOLDER_NAME)
    if not root_id:
        raise LookupError(
            f"Could not find '{CAMELOT_ROOT_FOLDER_NAME}' root folder in Google Drive. "
            "Create it manually and share it with the service account."
        )
    return root_id


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------

def list_unindexed_files(
    folder_id: str,
    recursive: bool = False,
    page_size: int = 100,
) -> list[dict]:
    """
    List files in a Drive folder that do not follow the MDS naming convention.

    Args:
        folder_id:   Google Drive folder ID to scan.
        recursive:   If True, also scan subfolders.
        page_size:   Number of files to fetch per API page.

    Returns:
        List of file dicts with id, name, mimeType, modifiedTime, parents.
        Only returns files that are NOT already MDS-compliant.
    """
    service = get_drive_service()
    query = (
        f"'{folder_id}' in parents "
        f"and mimeType != 'application/vnd.google-apps.folder' "
        f"and trashed = false"
    )

    all_files = []
    page_token = None

    try:
        while True:
            kwargs = {
                "q": query,
                "spaces": "drive",
                "fields": "nextPageToken, files(id, name, mimeType, modifiedTime, parents, size)",
                "pageSize": page_size,
            }
            if page_token:
                kwargs["pageToken"] = page_token

            result = service.files().list(**kwargs).execute()
            all_files.extend(result.get("files", []))
            page_token = result.get("nextPageToken")
            if not page_token:
                break
    except Exception as e:
        logger.error(f"Failed to list files in folder {folder_id}: {e}")
        return []

    # Filter to only unindexed files (not matching MDS pattern)
    unindexed = [f for f in all_files if not MDS_FILENAME_PATTERN.match(f["name"])]

    if recursive:
        # Also scan subfolders
        subfolder_query = (
            f"'{folder_id}' in parents "
            f"and mimeType = 'application/vnd.google-apps.folder' "
            f"and trashed = false"
        )
        try:
            subfolder_result = service.files().list(
                q=subfolder_query, fields="files(id, name)"
            ).execute()
            for subfolder in subfolder_result.get("files", []):
                unindexed.extend(list_unindexed_files(subfolder["id"], recursive=True))
        except Exception as e:
            logger.warning(f"Error scanning subfolders: {e}")

    logger.info(
        f"Found {len(all_files)} file(s) in folder {folder_id}, "
        f"{len(unindexed)} unindexed"
    )
    return unindexed


def rename_file(file_id: str, new_name: str) -> dict:
    """
    Rename a file in Google Drive.

    Args:
        file_id:   Google Drive file ID
        new_name:  New filename (with extension)

    Returns:
        Updated file metadata dict.
    """
    service = get_drive_service()
    try:
        updated = service.files().update(
            fileId=file_id,
            body={"name": new_name},
            fields="id, name, modifiedTime",
        ).execute()
        logger.info(f"Renamed file {file_id} → '{new_name}'")
        return updated
    except Exception as e:
        logger.error(f"Failed to rename file {file_id} to '{new_name}': {e}")
        raise


def move_to_correct_folder(
    file_id: str,
    mds_code: str,
    doc_type: str,
) -> dict:
    """
    Move a file to the correct MDS folder: /Camelot/{MDS_CODE}/{DOC_TYPE}/

    Args:
        file_id:   Google Drive file ID
        mds_code:  MDS building code (e.g., "552")
        doc_type:  Document type code (e.g., "LEASE")

    Returns:
        Updated file metadata dict.
    """
    service = get_drive_service()

    # Resolve folder hierarchy
    root_id = get_camelot_root_id()
    building_folder_id = get_or_create_folder(mds_code, root_id)
    doc_type_folder_id = get_or_create_folder(doc_type.upper(), building_folder_id)

    try:
        # Get current parents to remove them
        file_meta = service.files().get(
            fileId=file_id, fields="parents"
        ).execute()
        current_parents = ",".join(file_meta.get("parents", []))

        # Move: add new parent, remove old parents
        updated = service.files().update(
            fileId=file_id,
            addParents=doc_type_folder_id,
            removeParents=current_parents,
            fields="id, name, parents",
        ).execute()

        logger.info(
            f"Moved file {file_id} to /Camelot/{mds_code}/{doc_type}/ "
            f"(folder ID: {doc_type_folder_id})"
        )
        return updated
    except Exception as e:
        logger.error(f"Failed to move file {file_id}: {e}")
        raise


def move_to_unresolved(file_id: str, reason: str = "") -> dict:
    """Move a file to /Camelot/Incoming/UNRESOLVED/ when MDS code cannot be determined."""
    service = get_drive_service()
    root_id = get_camelot_root_id()
    incoming_id = get_or_create_folder(INCOMING_FOLDER_NAME, root_id)
    unresolved_id = get_or_create_folder(UNRESOLVED_FOLDER_NAME, incoming_id)

    try:
        file_meta = service.files().get(fileId=file_id, fields="parents").execute()
        current_parents = ",".join(file_meta.get("parents", []))
        updated = service.files().update(
            fileId=file_id,
            addParents=unresolved_id,
            removeParents=current_parents,
            fields="id, name",
        ).execute()
        logger.warning(
            f"Moved file {file_id} to UNRESOLVED. Reason: {reason}"
        )
        return updated
    except Exception as e:
        logger.error(f"Failed to move file {file_id} to UNRESOLVED: {e}")
        raise


# ---------------------------------------------------------------------------
# Index report
# ---------------------------------------------------------------------------

def generate_index_report(
    folder_id: str,
    mds_code: str,
    output_format: str = "both",  # "csv" | "markdown" | "both"
) -> dict:
    """
    Generate a file index report for a building's Drive folder.

    Args:
        folder_id:     Google Drive folder ID for the building
        mds_code:      MDS code for the building (used in report title)
        output_format: "csv", "markdown", or "both"

    Returns:
        Dict with keys: csv_content (str), markdown_content (str), file_count (int),
        doc_type_counts (dict), unindexed_count (int)
    """
    service = get_drive_service()

    # Gather all files recursively
    all_files = _list_all_files_recursive(folder_id, service)
    total = len(all_files)
    unindexed_count = sum(1 for f in all_files if not MDS_FILENAME_PATTERN.match(f.get("name", "")))

    # Count by doc type
    doc_type_counts: dict[str, int] = {}
    for f in all_files:
        parts = f.get("name", "").split("_")
        doc_type = parts[1] if len(parts) >= 3 else "UNCLASSIFIED"
        doc_type_counts[doc_type] = doc_type_counts.get(doc_type, 0) + 1

    # Build CSV
    csv_content = ""
    if output_format in ("csv", "both"):
        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf)
        writer.writerow(["Filename", "Doc Type", "MDS Code", "Modified", "Size (bytes)", "Drive ID", "Indexed"])
        for f in sorted(all_files, key=lambda x: x.get("name", "")):
            name = f.get("name", "")
            parts = name.split("_")
            ftype = parts[1] if len(parts) >= 3 else "UNCLASSIFIED"
            fmds = parts[0] if len(parts) >= 1 else "UNKNOWN"
            indexed = "Yes" if MDS_FILENAME_PATTERN.match(name) else "No"
            writer.writerow([
                name,
                ftype,
                fmds,
                f.get("modifiedTime", "")[:10],
                f.get("size", ""),
                f.get("id", ""),
                indexed,
            ])
        csv_content = csv_buf.getvalue()

    # Build Markdown
    md_content = ""
    if output_format in ("markdown", "both"):
        lines = [
            f"# File Index — Building {mds_code}",
            f"*Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} | "
            f"Total Files: {total} | Unindexed: {unindexed_count}*\n",
            "## Document Type Summary",
            "| Doc Type | Count |",
            "|----------|-------|",
        ]
        for dtype, count in sorted(doc_type_counts.items()):
            lines.append(f"| {dtype} | {count} |")
        lines.append("")
        lines.append("## File Listing")
        lines.append("| Filename | Modified | Indexed |")
        lines.append("|----------|----------|---------|")
        for f in sorted(all_files, key=lambda x: x.get("name", "")):
            name = f.get("name", "")
            modified = f.get("modifiedTime", "")[:10]
            indexed = "✓" if MDS_FILENAME_PATTERN.match(name) else "✗"
            lines.append(f"| {name} | {modified} | {indexed} |")
        md_content = "\n".join(lines)

    return {
        "mds_code": mds_code,
        "file_count": total,
        "unindexed_count": unindexed_count,
        "doc_type_counts": doc_type_counts,
        "csv_content": csv_content,
        "markdown_content": md_content,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def _list_all_files_recursive(folder_id: str, service) -> list[dict]:
    """Recursively list all files under a folder."""
    all_files = []
    query = f"'{folder_id}' in parents and trashed = false"
    page_token = None

    while True:
        kwargs = {
            "q": query,
            "fields": "nextPageToken, files(id, name, mimeType, modifiedTime, size)",
            "pageSize": 100,
        }
        if page_token:
            kwargs["pageToken"] = page_token
        result = service.files().list(**kwargs).execute()
        items = result.get("files", [])

        for item in items:
            if item["mimeType"] == "application/vnd.google-apps.folder":
                all_files.extend(_list_all_files_recursive(item["id"], service))
            else:
                all_files.append(item)

        page_token = result.get("nextPageToken")
        if not page_token:
            break

    return all_files


# ---------------------------------------------------------------------------
# High-level: process a single file end-to-end
# ---------------------------------------------------------------------------

def process_incoming_file(
    file_id: str,
    original_name: str,
    default_mds_code: Optional[str] = None,
    content_hint: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """
    Full pipeline: classify → generate MDS filename → rename → move.

    Args:
        file_id:           Drive file ID
        original_name:     Current filename
        default_mds_code:  Fallback MDS code if can't be inferred from name
        content_hint:      Optional content text for better classification
        dry_run:           If True, return proposed changes without executing

    Returns:
        Result dict with: original_name, new_name, mds_code, doc_type,
        moved_to_folder, actions_taken (or proposed in dry_run).
    """
    from index_bot.mds_mapper import suggest_filename_from_original

    suggestion = suggest_filename_from_original(
        original_name,
        content_hint=content_hint,
        default_mds_code=default_mds_code,
    )

    mds_code = suggestion["mds_code"]
    doc_type = suggestion["doc_type"]
    new_name = suggestion["suggested_filename"]
    confidence = suggestion["confidence"]

    result = {
        "file_id": file_id,
        "original_name": original_name,
        "new_name": new_name,
        "mds_code": mds_code,
        "doc_type": doc_type,
        "confidence": confidence,
        "actions_taken": [],
        "moved_to_folder": None,
        "dry_run": dry_run,
    }

    if dry_run:
        result["actions_taken"].append(f"[DRY RUN] Would rename to: {new_name}")
        result["actions_taken"].append(
            f"[DRY RUN] Would move to: /Camelot/{mds_code or 'UNKNOWN'}/{doc_type}/"
        )
        logger.info(f"[DRY RUN] {original_name} → {new_name} (MDS: {mds_code}, type: {doc_type})")
        return result

    if not mds_code:
        move_to_unresolved(file_id, reason=f"No MDS code found for: {original_name}")
        result["actions_taken"].append("Moved to UNRESOLVED — no MDS code determined")
        result["moved_to_folder"] = "UNRESOLVED"
        return result

    try:
        # Rename
        rename_file(file_id, new_name)
        result["actions_taken"].append(f"Renamed to: {new_name}")

        # Move
        move_to_correct_folder(file_id, mds_code, doc_type)
        result["moved_to_folder"] = f"/Camelot/{mds_code}/{doc_type}/"
        result["actions_taken"].append(f"Moved to: {result['moved_to_folder']}")

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error processing file {file_id} ({original_name}): {e}")

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    action = sys.argv[1] if len(sys.argv) > 1 else "help"

    if action == "list-unindexed":
        folder_id = sys.argv[2] if len(sys.argv) > 2 else None
        if not folder_id:
            print("Usage: python drive_organizer.py list-unindexed <folder_id>")
            sys.exit(1)
        files = list_unindexed_files(folder_id)
        print(f"Unindexed files ({len(files)}):")
        for f in files:
            print(f"  {f['id']}: {f['name']}")

    elif action == "index-report":
        folder_id = sys.argv[2] if len(sys.argv) > 2 else None
        mds_code = sys.argv[3] if len(sys.argv) > 3 else "UNKNOWN"
        if not folder_id:
            print("Usage: python drive_organizer.py index-report <folder_id> <mds_code>")
            sys.exit(1)
        report = generate_index_report(folder_id, mds_code)
        print(report["markdown_content"])
    else:
        print("Actions: list-unindexed, index-report")
