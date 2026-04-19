"""
main.py — Index Bot Entry Point
Camelot Property Management Services Corp

CLI tool to run Index Bot operations manually or on a schedule.

Usage:
    python main.py serve                          # Start API server
    python main.py scan <folder_id>               # Scan folder for unindexed files
    python main.py process <folder_id> [mds_code] # Process all unindexed files in folder
    python main.py index <mds_code> [folder_id]   # Generate index report for a building
    python main.py classify <filename>             # Classify a filename
    python main.py buildings                       # List all MDS building codes

Author: Camelot OS
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("LOG_FILE", "logs/index_bot.log")
Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("index_bot.main")


def cmd_serve(args):
    """Start the FastAPI API server."""
    from index_bot.api_server import app
    import uvicorn
    host = os.getenv("API_HOST", args.host)
    port = int(os.getenv("API_PORT", str(args.port)))
    logger.info(f"Starting Index Bot API server on {host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level=LOG_LEVEL.lower())


def cmd_scan(args):
    """Scan a Drive folder for unindexed files."""
    from index_bot.drive_organizer import list_unindexed_files
    files = list_unindexed_files(args.folder_id, recursive=args.recursive)
    print(f"\nUnindexed files in folder {args.folder_id}: {len(files)}\n")
    for f in files:
        print(f"  [{f.get('id', '')}] {f.get('name', '')}")
    return files


def cmd_process(args):
    """Process all unindexed files in a Drive folder."""
    from index_bot.drive_organizer import list_unindexed_files, process_incoming_file
    files = list_unindexed_files(args.folder_id)
    print(f"\nProcessing {len(files)} unindexed file(s)...")

    results = []
    for f in files:
        result = process_incoming_file(
            file_id=f["id"],
            original_name=f["name"],
            default_mds_code=args.mds_code,
            dry_run=args.dry_run,
        )
        status = "✓" if result.get("success", not result.get("error")) else "✗"
        print(
            f"  {status} {f['name']!r:50} → "
            f"{result.get('new_name', 'ERROR')!r} "
            f"[{result.get('mds_code', '?')} / {result.get('doc_type', '?')}]"
        )
        results.append(result)

    success = sum(1 for r in results if not r.get("error"))
    print(f"\nComplete: {success}/{len(results)} files processed successfully")
    return results


def cmd_index(args):
    """Generate index report for a building."""
    from index_bot.drive_organizer import get_camelot_root_id, find_folder, generate_index_report

    folder_id = args.folder_id
    if not folder_id:
        root_id = get_camelot_root_id()
        folder_id = find_folder(args.mds_code, root_id)
        if not folder_id:
            print(f"ERROR: Building folder for MDS code '{args.mds_code}' not found in Drive")
            sys.exit(1)

    report = generate_index_report(
        folder_id=folder_id,
        mds_code=args.mds_code,
        output_format=args.format,
    )

    if args.format in ("csv", "both"):
        csv_path = f"logs/{args.mds_code}_index.csv"
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(report["csv_content"])
        print(f"CSV index saved to: {csv_path}")

    if args.format in ("markdown", "both"):
        print("\n" + report["markdown_content"])


def cmd_classify(args):
    """Classify a filename using MDS rules."""
    from index_bot.mds_mapper import suggest_filename_from_original
    suggestion = suggest_filename_from_original(
        args.filename,
        default_mds_code=args.mds_code,
    )
    print(json.dumps(suggestion, indent=2, default=str))


def cmd_buildings(args):
    """List all registered MDS building codes."""
    from index_bot.mds_mapper import list_all_buildings
    buildings = list_all_buildings()
    print(f"\nRegistered MDS Buildings ({len(buildings)}):\n")
    for b in buildings:
        aliases = ", ".join(b["aliases"][:3])
        print(f"  MDS {b['mds_code']:10} ← {aliases}")


def main():
    parser = argparse.ArgumentParser(
        description="Camelot Index Bot — Google Drive file organizer using MDS codes"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command")

    # serve
    p_serve = subparsers.add_parser("serve", help="Start API server")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8002)
    p_serve.set_defaults(func=cmd_serve)

    # scan
    p_scan = subparsers.add_parser("scan", help="Scan folder for unindexed files")
    p_scan.add_argument("folder_id", help="Google Drive folder ID")
    p_scan.add_argument("--recursive", action="store_true", help="Scan subfolders too")
    p_scan.set_defaults(func=cmd_scan)

    # process
    p_process = subparsers.add_parser("process", help="Process unindexed files in a folder")
    p_process.add_argument("folder_id", help="Google Drive folder ID")
    p_process.add_argument("mds_code", nargs="?", default=None, help="Default MDS code")
    p_process.add_argument("--dry-run", action="store_true", help="Preview only — do not modify files")
    p_process.set_defaults(func=cmd_process)

    # index
    p_index = subparsers.add_parser("index", help="Generate index report for a building")
    p_index.add_argument("mds_code", help="MDS building code")
    p_index.add_argument("folder_id", nargs="?", default=None, help="Google Drive folder ID (optional)")
    p_index.add_argument("--format", choices=["csv", "markdown", "both"], default="markdown")
    p_index.set_defaults(func=cmd_index)

    # classify
    p_classify = subparsers.add_parser("classify", help="Classify a filename")
    p_classify.add_argument("filename", help="Filename to classify")
    p_classify.add_argument("--mds-code", default=None, help="Default MDS code")
    p_classify.set_defaults(func=cmd_classify)

    # buildings
    p_buildings = subparsers.add_parser("buildings", help="List all MDS building codes")
    p_buildings.set_defaults(func=cmd_buildings)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
