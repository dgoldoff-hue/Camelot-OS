"""
main.py — Camelot OS Report Bot
================================
Primary entry point for the Report Bot. Provides both a CLI interface
for direct invocation and a FastAPI HTTP endpoint for webhook / agent
orchestration triggers.

Endpoints (when run as FastAPI):
  POST /reports/owner-statements       — generate all owner statements
  POST /reports/kpi-dashboard          — generate KPI dashboard
  POST /reports/investor-update        — generate investor update PDF
  GET  /reports/status                 — health check

CLI usage:
  python main.py owner-statements
  python main.py kpi-dashboard
  python main.py investor-update [--year YYYY --quarter Q]
  python main.py scheduler             — start persistent cron daemon
  python main.py serve                 — start FastAPI server (port 8003)

Author: Camelot OS
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Logging — configured before any module imports that might log
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("LOG_FILE", "logs/report_bot.log")

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
logger = logging.getLogger("report_bot.main")

# ---------------------------------------------------------------------------
# Lazy imports — only load what's needed for the selected command
# ---------------------------------------------------------------------------

OUTPUT_DIR: str = os.getenv("REPORT_OUTPUT_DIR", "output/reports")


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def cmd_owner_statements(args: argparse.Namespace) -> int:
    """Generate all owner statements for the current month."""
    from datetime import date

    from owner_statement import generate_all_statements

    today = date.today()
    output_subdir = os.path.join(OUTPUT_DIR, "owner_statements", today.strftime("%Y-%m"))
    os.makedirs(output_subdir, exist_ok=True)

    logger.info("Generating owner statements → %s", output_subdir)
    try:
        results = generate_all_statements(output_dir=output_subdir)
        logger.info("Generated %d owner statements", len(results))
        for r in results:
            print(f"  ✓ {r.get('property_name', '?'):40s}  {r.get('pdf_path', '')}")
        return 0
    except Exception as exc:
        logger.exception("Owner statements failed: %s", exc)
        return 1


def cmd_kpi_dashboard(args: argparse.Namespace) -> int:
    """Generate the weekly KPI dashboard."""
    from kpi_dashboard import generate_kpi_dashboard

    output_subdir = os.path.join(OUTPUT_DIR, "kpi_dashboards")
    os.makedirs(output_subdir, exist_ok=True)

    logger.info("Generating KPI dashboard → %s", output_subdir)
    try:
        results = generate_kpi_dashboard(
            output_dir=output_subdir,
            persist=not getattr(args, "no_persist", False),
        )
        print(f"Markdown: {results['markdown_path']}")
        print(f"PDF:      {results['pdf_path']}")
        return 0
    except Exception as exc:
        logger.exception("KPI dashboard failed: %s", exc)
        return 1


def cmd_investor_update(args: argparse.Namespace) -> int:
    """Generate the quarterly investor update."""
    from investor_update import QuarterPeriod, generate_investor_update

    year: Optional[int] = getattr(args, "year", None)
    qnum: Optional[int] = getattr(args, "quarter", None)

    q: Optional[QuarterPeriod] = None
    if year and qnum:
        q = QuarterPeriod(year=year, quarter=qnum)

    output_subdir = os.path.join(OUTPUT_DIR, "investor_updates")
    os.makedirs(output_subdir, exist_ok=True)

    logger.info("Generating investor update → %s", output_subdir)
    try:
        pdf_path = generate_investor_update(quarter=q, output_dir=output_subdir)
        print(f"PDF: {pdf_path}")
        return 0
    except Exception as exc:
        logger.exception("Investor update failed: %s", exc)
        return 1


def cmd_scheduler(args: argparse.Namespace) -> int:
    """Start the persistent cron scheduler daemon."""
    from scheduler import run_daemon

    logger.info("Starting Report Bot scheduler daemon")
    run_daemon()  # blocks until SIGTERM/SIGINT
    return 0


# ---------------------------------------------------------------------------
# FastAPI server
# ---------------------------------------------------------------------------

def build_app() -> Any:
    """
    Build and return the FastAPI application.
    Import is deferred so the CLI works without fastapi installed.
    """
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import FileResponse, JSONResponse
    from pydantic import BaseModel

    app = FastAPI(
        title="Camelot Report Bot",
        description="Automated report generation for Camelot Property Management",
        version="1.0.0",
    )

    # ── Request models ────────────────────────────────────────────────────

    class OwnerStatementsRequest(BaseModel):
        year: Optional[int] = None
        month: Optional[int] = None

    class KPIDashboardRequest(BaseModel):
        persist: bool = True

    class InvestorUpdateRequest(BaseModel):
        year: Optional[int] = None
        quarter: Optional[int] = None
        market_commentary: Optional[str] = None
        outlook: Optional[str] = None

    # ── Routes ────────────────────────────────────────────────────────────

    @app.get("/reports/status")
    async def health_check() -> dict[str, str]:
        return {
            "status": "ok",
            "service": "Camelot Report Bot",
            "timestamp": datetime.utcnow().isoformat(),
        }

    @app.post("/reports/owner-statements")
    async def owner_statements(req: OwnerStatementsRequest) -> JSONResponse:
        """Generate owner statements for all properties."""
        from datetime import date

        from owner_statement import generate_all_statements

        target = date.today()
        if req.year and req.month:
            try:
                target = date(req.year, req.month, 1)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        output_subdir = os.path.join(
            OUTPUT_DIR, "owner_statements", target.strftime("%Y-%m")
        )
        os.makedirs(output_subdir, exist_ok=True)

        try:
            results = generate_all_statements(output_dir=output_subdir)
            return JSONResponse(
                {
                    "status": "success",
                    "count": len(results),
                    "output_dir": output_subdir,
                    "statements": [
                        {
                            "property": r.get("property_name"),
                            "pdf": r.get("pdf_path"),
                        }
                        for r in results
                    ],
                }
            )
        except Exception as exc:
            logger.exception("API owner-statements failed: %s", exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.post("/reports/kpi-dashboard")
    async def kpi_dashboard(req: KPIDashboardRequest) -> JSONResponse:
        """Generate the KPI dashboard."""
        from kpi_dashboard import generate_kpi_dashboard

        output_subdir = os.path.join(OUTPUT_DIR, "kpi_dashboards")
        os.makedirs(output_subdir, exist_ok=True)

        try:
            results = generate_kpi_dashboard(
                output_dir=output_subdir, persist=req.persist
            )
            return JSONResponse(
                {
                    "status": "success",
                    "markdown_path": results["markdown_path"],
                    "pdf_path": results["pdf_path"],
                }
            )
        except Exception as exc:
            logger.exception("API kpi-dashboard failed: %s", exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.post("/reports/investor-update")
    async def investor_update(req: InvestorUpdateRequest) -> JSONResponse:
        """Generate the investor update PDF."""
        from investor_update import QuarterPeriod, generate_investor_update

        q: Optional[QuarterPeriod] = None
        if req.year and req.quarter:
            q = QuarterPeriod(year=req.year, quarter=req.quarter)

        output_subdir = os.path.join(OUTPUT_DIR, "investor_updates")
        os.makedirs(output_subdir, exist_ok=True)

        try:
            pdf_path = generate_investor_update(
                quarter=q,
                output_dir=output_subdir,
                market_commentary=req.market_commentary,
                outlook=req.outlook,
            )
            return JSONResponse({"status": "success", "pdf_path": pdf_path})
        except Exception as exc:
            logger.exception("API investor-update failed: %s", exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    return app


def cmd_serve(args: argparse.Namespace) -> int:
    """Start the FastAPI HTTP server."""
    try:
        import uvicorn  # type: ignore
    except ImportError:
        print("uvicorn is required for --serve mode. Install it: pip install uvicorn")
        return 1

    port = int(os.getenv("REPORT_BOT_PORT", "8003"))
    host = os.getenv("REPORT_BOT_HOST", "0.0.0.0")

    logger.info("Starting Report Bot HTTP server on %s:%d", host, port)
    app = build_app()
    uvicorn.run(app, host=host, port=port, log_level=LOG_LEVEL.lower())
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Camelot OS Report Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  owner-statements    Generate all owner statements for current month
  kpi-dashboard       Generate weekly KPI dashboard (Markdown + PDF)
  investor-update     Generate quarterly investor update PDF
  scheduler           Start the persistent cron scheduler daemon
  serve               Start the FastAPI HTTP server on port 8003

Examples:
  python main.py owner-statements
  python main.py kpi-dashboard
  python main.py investor-update --year 2025 --quarter 3
  python main.py scheduler
  python main.py serve
        """,
    )

    subparsers = parser.add_subparsers(dest="command")

    # owner-statements
    p_stmts = subparsers.add_parser("owner-statements", help="Generate owner statements")
    p_stmts.add_argument("--year", type=int, default=None)
    p_stmts.add_argument("--month", type=int, default=None)

    # kpi-dashboard
    p_kpi = subparsers.add_parser("kpi-dashboard", help="Generate KPI dashboard")
    p_kpi.add_argument(
        "--no-persist",
        action="store_true",
        help="Skip saving snapshot to Supabase",
    )

    # investor-update
    p_inv = subparsers.add_parser("investor-update", help="Generate investor update")
    p_inv.add_argument("--year", type=int, default=None)
    p_inv.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], default=None)

    # scheduler
    subparsers.add_parser("scheduler", help="Start cron scheduler daemon")

    # serve
    subparsers.add_parser("serve", help="Start FastAPI HTTP server")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    dispatch = {
        "owner-statements": cmd_owner_statements,
        "kpi-dashboard": cmd_kpi_dashboard,
        "investor-update": cmd_investor_update,
        "scheduler": cmd_scheduler,
        "serve": cmd_serve,
    }

    handler = dispatch.get(args.command)
    if handler is None:
        parser.print_help()
        sys.exit(1)

    sys.exit(handler(args))


if __name__ == "__main__":
    main()
