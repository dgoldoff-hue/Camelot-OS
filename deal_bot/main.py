"""
main.py — Camelot OS Deal Bot
==============================
Primary entry point for the Deal Bot. Provides a CLI interface and a
FastAPI HTTP server for webhook / agent orchestration.

Commands:
  prospect    Research a target company or owner and build a prospect profile
  outreach    Generate a personalized outreach email
  sequence    Create and enqueue a 5-email drip sequence
  sequences   Process (send) all pending sequence emails due today
  battlecard  Generate a pre-meeting battlecard (PDF + Markdown)
  hubspot     HubSpot pipeline operations (summary, upsert, stage)
  serve       Start the FastAPI HTTP server (port 8004)

Author: Camelot OS
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Logging — set up before other imports that may log
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("LOG_FILE", "logs/deal_bot.log")

import pathlib
pathlib.Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("deal_bot.main")

OUTPUT_DIR: str = os.getenv("DEAL_BOT_OUTPUT_DIR", "output/deal_bot")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_profile(profile_path: str) -> Any:
    """Load a ProspectProfile from a JSON file."""
    from dataclasses import fields as dc_fields
    from prospect_mapper import ProspectProfile

    with open(profile_path) as f:
        data = json.load(f)
    valid_keys = {f.name for f in dc_fields(ProspectProfile)}
    return ProspectProfile(**{k: v for k, v in data.items() if k in valid_keys})


def _save_profile(profile: Any, output_dir: str) -> str:
    """Save a ProspectProfile as JSON and return the path."""
    os.makedirs(output_dir, exist_ok=True)
    safe_name = "".join(
        c if c.isalnum() or c in (" ", "-", "_") else "_"
        for c in profile.company_name
    ).strip()[:50]
    path = os.path.join(output_dir, f"prospect_{safe_name}.json")
    with open(path, "w", encoding="utf-8") as f:
        f.write(profile.to_json())
    logger.info("Profile saved to %s", path)
    return path


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def cmd_prospect(args: argparse.Namespace) -> int:
    """Research a target company/owner and build a ProspectProfile."""
    from prospect_mapper import ProspectMapper, batch_research

    output_dir = os.path.join(OUTPUT_DIR, "profiles")

    if getattr(args, "batch", None):
        with open(args.batch) as f:
            targets = json.load(f)
        batch_output = os.path.join(output_dir, "batch_results.json")
        profiles = batch_research(
            targets,
            output_path=batch_output,
            enrich_email=not getattr(args, "no_email", False),
        )
        print(f"\nResearched {len(profiles)} prospects:")
        for p in profiles:
            print(f"  [{p.fit_score:5.1f}] {p.company_name:40s} {p.estimated_units:4d} units  {p.recommended_angle}")
        print(f"\nResults saved to: {batch_output}")
        return 0

    if not getattr(args, "company", None) and not getattr(args, "owner", None):
        print("Error: --company or --owner is required (or --batch for batch mode)")
        return 1

    mapper = ProspectMapper()
    city = getattr(args, "city", "New York") or "New York"
    enrich = not getattr(args, "no_email", False)

    if getattr(args, "company", None):
        profile = mapper.research_by_company(args.company, city=city, enrich_email=enrich)
    else:
        profile = mapper.research_by_owner(args.owner, city=city, enrich_email=enrich)

    profile_path = _save_profile(profile, output_dir)
    print(json.loads(profile.to_json()))
    print(f"\nProfile saved to: {profile_path}")
    print(f"Fit score: {profile.fit_score}  Angle: {profile.recommended_angle}  Structure: {profile.recommended_structure}")
    return 0


def cmd_outreach(args: argparse.Namespace) -> int:
    """Generate a personalized outreach email for a prospect."""
    from outreach_generator import OutreachGenerator

    profile = _load_profile(args.profile)
    gen = OutreachGenerator()

    if getattr(args, "all_angles", False):
        emails = gen.generate_all_angles(profile, contact_name=getattr(args, "contact", None))
        for e in emails:
            print(f"\n{'='*60}")
            print(f"Angle: {e.angle} | Structure: {e.structure}")
            print(f"{'='*60}")
            print(e)
    else:
        email = gen.generate(
            profile,
            angle=getattr(args, "angle", None),
            structure=getattr(args, "structure", None),
            contact_name=getattr(args, "contact", None),
        )
        print(email)

        if getattr(args, "output", None):
            os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(email.to_dict(), f, indent=2)
            print(f"\nEmail saved to: {args.output}")

    return 0


def cmd_sequence(args: argparse.Namespace) -> int:
    """Create and enqueue a 5-email drip sequence for a prospect."""
    from email_sequences import create_and_enqueue_sequence

    profile = _load_profile(args.profile)

    if getattr(args, "preview", False):
        from email_sequences import SequenceBuilder
        builder = SequenceBuilder()
        seq = builder.build(
            profile=profile,
            prospect_email=args.email,
        )
        for e in seq.emails:
            print(f"\n{'='*60}")
            print(f"STEP {e.step_number} — Day {e.day_offset} — Scheduled: {e.scheduled_date}")
            print(f"{'='*60}")
            print(f"Subject: {e.subject}\n")
            print(e.body)
        return 0

    seq = create_and_enqueue_sequence(
        profile=profile,
        prospect_email=args.email,
        hubspot_deal_id=getattr(args, "deal_id", "") or "",
        angle=getattr(args, "angle", None),
        structure=getattr(args, "structure", None),
    )
    print(f"Sequence created: {seq.sequence_id}")
    print(f"Prospect: {seq.prospect_name} <{seq.prospect_email}>")
    for e in seq.emails:
        print(f"  Step {e.step_number} — Day {e.day_offset} — {e.scheduled_date}: {e.subject}")
    return 0


def cmd_sequences_run(args: argparse.Namespace) -> int:
    """Process and send all pending sequence emails due today."""
    from email_sequences import run_pending_sequences

    logger.info("Running pending email sequences")
    counts = run_pending_sequences()
    print(f"Sent: {counts['sent']}  Failed: {counts['failed']}  Skipped: {counts['skipped']}")
    return 0 if counts["failed"] == 0 else 1


def cmd_battlecard(args: argparse.Namespace) -> int:
    """Generate a battlecard (PDF + Markdown) for a prospect."""
    from battlecard_generator import generate_battlecard

    profile = _load_profile(args.profile)
    output_dir = getattr(args, "output_dir", None) or os.path.join(OUTPUT_DIR, "battlecards")
    formats = getattr(args, "format", None) or ["pdf", "md"]

    if getattr(args, "preview", False):
        from battlecard_generator import build_battlecard
        bc = build_battlecard(profile)
        print(bc.to_markdown())
        return 0

    results = generate_battlecard(
        profile=profile,
        output_dir=output_dir,
        formats=formats,
    )
    for key, path in results.items():
        print(f"{key}: {path}")
    return 0


def cmd_hubspot(args: argparse.Namespace) -> int:
    """HubSpot pipeline operations."""
    import subprocess

    subcommand = getattr(args, "subcommand", "summary")
    node_args = ["node", os.path.join(os.path.dirname(__file__), "hubspot_pipeline.js")]

    if subcommand == "summary":
        node_args.append("summary")
    elif subcommand == "stage":
        node_args.extend(["stage", getattr(args, "stage_name", "Identified")])
    elif subcommand == "get":
        node_args.extend(["get", args.deal_id])
    elif subcommand == "search":
        node_args.extend(["search"] + args.query.split())
    elif subcommand == "upsert":
        # Read profile JSON and upsert to HubSpot via inline Node script
        profile = _load_profile(args.profile)
        script = f"""
const {{ upsertProspect }} = require('{os.path.join(os.path.dirname(__file__), "hubspot_pipeline.js")}');
const data = {profile.to_json()};
upsertProspect(data).then(id => console.log('Deal ID:', id)).catch(console.error);
"""
        tmp_script = "/tmp/camelot_deal_upsert.js"
        with open(tmp_script, "w") as f:
            f.write(script)
        node_args = ["node", tmp_script]
    else:
        print(f"Unknown hubspot subcommand: {subcommand}")
        return 1

    try:
        result = subprocess.run(node_args, capture_output=False, text=True)
        return result.returncode
    except FileNotFoundError:
        print("Error: Node.js not found. Install Node.js to use HubSpot commands.")
        return 1


# ---------------------------------------------------------------------------
# FastAPI server
# ---------------------------------------------------------------------------

def build_app() -> Any:
    """Build and return the FastAPI application."""
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import FileResponse, JSONResponse
    from pydantic import BaseModel

    app = FastAPI(
        title="Camelot Deal Bot",
        description="Acquisition pipeline management for Camelot Property Management",
        version="1.0.0",
    )

    class ProspectRequest(BaseModel):
        company_name: Optional[str] = None
        owner_name: Optional[str] = None
        city: str = "New York"
        enrich_email: bool = True

    class OutreachRequest(BaseModel):
        profile_path: str
        angle: Optional[str] = None
        structure: Optional[str] = None
        contact_name: Optional[str] = None

    class SequenceRequest(BaseModel):
        profile_path: str
        prospect_email: str
        hubspot_deal_id: str = ""
        angle: Optional[str] = None
        structure: Optional[str] = None

    class BattlecardRequest(BaseModel):
        profile_path: str
        formats: list[str] = ["pdf", "md"]

    @app.get("/deal/status")
    async def health_check() -> dict[str, str]:
        from datetime import datetime
        return {
            "status": "ok",
            "service": "Camelot Deal Bot",
            "timestamp": datetime.utcnow().isoformat(),
        }

    @app.post("/deal/prospect")
    async def research_prospect(req: ProspectRequest) -> JSONResponse:
        """Research a prospect and return the profile."""
        from prospect_mapper import ProspectMapper

        if not req.company_name and not req.owner_name:
            raise HTTPException(status_code=400, detail="company_name or owner_name required")

        mapper = ProspectMapper()
        try:
            if req.company_name:
                profile = mapper.research_by_company(
                    req.company_name, city=req.city, enrich_email=req.enrich_email
                )
            else:
                profile = mapper.research_by_owner(
                    req.owner_name, city=req.city, enrich_email=req.enrich_email
                )

            profile_dir = os.path.join(OUTPUT_DIR, "profiles")
            profile_path = _save_profile(profile, profile_dir)
            return JSONResponse({"status": "success", "profile": profile.to_dict(), "profile_path": profile_path})
        except Exception as exc:
            logger.exception("Prospect research failed: %s", exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.post("/deal/outreach")
    async def generate_outreach(req: OutreachRequest) -> JSONResponse:
        """Generate an outreach email from a prospect profile."""
        from outreach_generator import OutreachGenerator

        try:
            profile = _load_profile(req.profile_path)
            gen = OutreachGenerator()
            email = gen.generate(
                profile,
                angle=req.angle,
                structure=req.structure,
                contact_name=req.contact_name,
            )
            return JSONResponse({"status": "success", "email": email.to_dict()})
        except Exception as exc:
            logger.exception("Outreach generation failed: %s", exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.post("/deal/sequence")
    async def create_sequence(req: SequenceRequest) -> JSONResponse:
        """Create and enqueue an email sequence for a prospect."""
        from email_sequences import create_and_enqueue_sequence

        try:
            profile = _load_profile(req.profile_path)
            seq = create_and_enqueue_sequence(
                profile=profile,
                prospect_email=req.prospect_email,
                hubspot_deal_id=req.hubspot_deal_id,
                angle=req.angle,
                structure=req.structure,
            )
            return JSONResponse({
                "status": "success",
                "sequence_id": seq.sequence_id,
                "email_count": len(seq.emails),
                "start_date": seq.start_date,
                "emails": [
                    {"step": e.step_number, "day": e.day_offset, "date": e.scheduled_date, "subject": e.subject}
                    for e in seq.emails
                ],
            })
        except Exception as exc:
            logger.exception("Sequence creation failed: %s", exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.post("/deal/sequences/run")
    async def run_sequences() -> JSONResponse:
        """Send all pending emails due today."""
        from email_sequences import run_pending_sequences
        counts = run_pending_sequences()
        return JSONResponse({"status": "success", **counts})

    @app.post("/deal/battlecard")
    async def create_battlecard(req: BattlecardRequest) -> JSONResponse:
        """Generate a battlecard for a prospect."""
        from battlecard_generator import generate_battlecard

        try:
            profile = _load_profile(req.profile_path)
            output_dir = os.path.join(OUTPUT_DIR, "battlecards")
            results = generate_battlecard(profile=profile, output_dir=output_dir, formats=req.formats)
            return JSONResponse({"status": "success", **results})
        except Exception as exc:
            logger.exception("Battlecard generation failed: %s", exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    return app


def cmd_serve(args: argparse.Namespace) -> int:
    """Start the FastAPI HTTP server."""
    try:
        import uvicorn
    except ImportError:
        print("uvicorn is required for serve mode. Install: pip install uvicorn")
        return 1

    port = int(os.getenv("DEAL_BOT_PORT", "8004"))
    host = os.getenv("DEAL_BOT_HOST", "0.0.0.0")
    logger.info("Starting Deal Bot HTTP server on %s:%d", host, port)
    app = build_app()
    uvicorn.run(app, host=host, port=port, log_level=LOG_LEVEL.lower())
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Camelot OS Deal Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  prospect      Research a target company/owner
  outreach      Generate personalized outreach email
  sequence      Create a 5-email drip sequence
  sequences     Send pending sequence emails (run daily)
  battlecard    Generate pre-meeting battlecard
  hubspot       HubSpot pipeline operations
  serve         Start FastAPI HTTP server on port 8004

Examples:
  python main.py prospect --company "Acme Property Management" --city "Bronx"
  python main.py outreach --profile output/profiles/prospect_Acme.json
  python main.py sequence --profile output/profiles/prospect_Acme.json --email owner@acme.com
  python main.py sequences
  python main.py battlecard --profile output/profiles/prospect_Acme.json
  python main.py hubspot summary
  python main.py serve
        """,
    )
    subparsers = parser.add_subparsers(dest="command")

    # prospect
    p_prospect = subparsers.add_parser("prospect", help="Research a target company")
    group = p_prospect.add_mutually_exclusive_group()
    group.add_argument("--company", help="Company name")
    group.add_argument("--owner", help="Owner/principal name")
    p_prospect.add_argument("--batch", help="Path to JSON array of targets")
    p_prospect.add_argument("--city", default="New York")
    p_prospect.add_argument("--no-email", action="store_true", help="Skip email enrichment")

    # outreach
    p_outreach = subparsers.add_parser("outreach", help="Generate outreach email")
    p_outreach.add_argument("--profile", required=True, help="Prospect JSON path")
    p_outreach.add_argument("--angle", choices=["succession", "growth", "systems-upgrade", "tired-operator"])
    p_outreach.add_argument("--structure", choices=["equity-sale", "roll-up", "powered-by"])
    p_outreach.add_argument("--contact", help="Contact name override")
    p_outreach.add_argument("--all-angles", action="store_true")
    p_outreach.add_argument("--output", help="Save email JSON to path")

    # sequence
    p_seq = subparsers.add_parser("sequence", help="Create drip email sequence")
    p_seq.add_argument("--profile", required=True, help="Prospect JSON path")
    p_seq.add_argument("--email", required=True, help="Recipient email address")
    p_seq.add_argument("--deal-id", default="", help="HubSpot deal ID")
    p_seq.add_argument("--angle", choices=["succession", "growth", "systems-upgrade", "tired-operator"])
    p_seq.add_argument("--structure", choices=["equity-sale", "roll-up", "powered-by"])
    p_seq.add_argument("--preview", action="store_true", help="Preview without sending")

    # sequences run
    subparsers.add_parser("sequences", help="Send all pending sequence emails due today")

    # battlecard
    p_bc = subparsers.add_parser("battlecard", help="Generate battlecard")
    p_bc.add_argument("--profile", required=True, help="Prospect JSON path")
    p_bc.add_argument("--output-dir", default=None)
    p_bc.add_argument("--format", nargs="+", choices=["pdf", "md"], default=["pdf", "md"])
    p_bc.add_argument("--preview", action="store_true", help="Print Markdown to stdout")

    # hubspot
    p_hs = subparsers.add_parser("hubspot", help="HubSpot pipeline operations")
    hs_sub = p_hs.add_subparsers(dest="subcommand")
    hs_sub.add_parser("summary", help="Pipeline summary by stage")
    p_hs_stage = hs_sub.add_parser("stage", help="List deals in a stage")
    p_hs_stage.add_argument("stage_name", nargs="?", default="Identified")
    p_hs_get = hs_sub.add_parser("get", help="Get deal details")
    p_hs_get.add_argument("deal_id")
    p_hs_search = hs_sub.add_parser("search", help="Search deals by name")
    p_hs_search.add_argument("query")
    p_hs_upsert = hs_sub.add_parser("upsert", help="Upsert prospect from JSON")
    p_hs_upsert.add_argument("--profile", required=True)

    # serve
    subparsers.add_parser("serve", help="Start FastAPI server")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    dispatch = {
        "prospect":   cmd_prospect,
        "outreach":   cmd_outreach,
        "sequence":   cmd_sequence,
        "sequences":  cmd_sequences_run,
        "battlecard": cmd_battlecard,
        "hubspot":    cmd_hubspot,
        "serve":      cmd_serve,
    }

    handler = dispatch.get(args.command)
    if handler is None:
        parser.print_help()
        sys.exit(1)

    sys.exit(handler(args))


if __name__ == "__main__":
    main()
