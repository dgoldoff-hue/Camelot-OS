"""
api_server.py — Index Bot FastAPI Server
Camelot Property Management Services Corp / Index Bot

Exposes Index Bot as a REST API for Make.com webhooks and direct integration.

Endpoints:
    POST /classify        — Classify a file, return MDS code + doc type + new filename
    POST /rename          — Rename + move a file in Drive
    POST /make/incoming   — Full Make.com webhook handler (classify + rename + move + log)
    GET  /index/{mds_code} — Return file index for a building
    GET  /buildings        — List all registered MDS building codes
    GET  /health           — Health check

Run:
    uvicorn index_bot.api_server:app --host 0.0.0.0 --port 8002 --reload

Author: Camelot OS
"""

import logging
import os
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from fastapi import FastAPI, HTTPException, Header, Request, Query
    from fastapi.responses import JSONResponse, PlainTextResponse
    from pydantic import BaseModel, Field
    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False
    logger.warning("FastAPI not installed. Run: pip install fastapi uvicorn pydantic")

WEBHOOK_SECRET = os.getenv("MAKE_WEBHOOK_SECRET", "")


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------

if _FASTAPI_AVAILABLE:
    class ClassifyRequest(BaseModel):
        file_name: str = Field(..., description="Original filename")
        file_id: Optional[str] = Field(None, description="Google Drive file ID")
        content_hint: Optional[str] = Field(None, description="Optional extracted text or description")
        default_mds_code: Optional[str] = Field(None, description="Fallback MDS code if not determinable from filename")

    class ClassifyResponse(BaseModel):
        file_name: str
        mds_code: Optional[str]
        doc_type: str
        doc_date: str
        suggested_filename: str
        confidence: str
        version: int

    class RenameRequest(BaseModel):
        file_id: str = Field(..., description="Google Drive file ID")
        file_name: str = Field(..., description="Current filename")
        mds_code: Optional[str] = Field(None, description="Override MDS code")
        doc_type: Optional[str] = Field(None, description="Override doc type")
        doc_date: Optional[str] = Field(None, description="Override date (YYYY-MM-DD)")
        version: int = Field(1, description="File version number")
        dry_run: bool = Field(False, description="If true, do not execute — return proposed changes")

    class MakeWebhookRequest(BaseModel):
        file_id: str
        file_name: str
        folder_id: Optional[str] = None
        folder_name: Optional[str] = None
        created_by: Optional[str] = None
        content_hint: Optional[str] = None
        default_mds_code: Optional[str] = None
        dry_run: bool = False
        mime_type: Optional[str] = None
        size: Optional[int] = None


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> "FastAPI":
    if not _FASTAPI_AVAILABLE:
        raise ImportError("FastAPI required: pip install fastapi uvicorn pydantic")

    app = FastAPI(
        title="Camelot Index Bot API",
        description="File classification and Drive organization API for Camelot OS",
        version="1.0.0",
    )

    # ---------------------------------------------------------------------------
    # Health
    # ---------------------------------------------------------------------------

    @app.get("/health", tags=["System"])
    async def health():
        """Service health check."""
        return {
            "status": "ok",
            "service": "Camelot Index Bot",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

    # ---------------------------------------------------------------------------
    # POST /classify
    # ---------------------------------------------------------------------------

    @app.post("/classify", response_model=ClassifyResponse, tags=["Classification"])
    async def classify_file(request: ClassifyRequest):
        """
        Classify a file and return the suggested MDS filename.
        Does NOT modify the file — read-only classification.
        """
        from index_bot.mds_mapper import suggest_filename_from_original
        try:
            suggestion = suggest_filename_from_original(
                request.file_name,
                content_hint=request.content_hint,
                default_mds_code=request.default_mds_code,
            )
            return ClassifyResponse(
                file_name=request.file_name,
                mds_code=suggestion["mds_code"],
                doc_type=suggestion["doc_type"],
                doc_date=suggestion["doc_date"],
                suggested_filename=suggestion["suggested_filename"],
                confidence=suggestion["confidence"],
                version=suggestion["version"],
            )
        except Exception as e:
            logger.error(f"Classification error: {e}")
            raise HTTPException(status_code=500, detail=f"Classification failed: {str(e)}")

    # ---------------------------------------------------------------------------
    # POST /rename
    # ---------------------------------------------------------------------------

    @app.post("/rename", tags=["Drive Operations"])
    async def rename_and_move(request: RenameRequest):
        """
        Rename and move a Google Drive file to the correct MDS folder.
        If mds_code is not provided, it's inferred from file_name.
        """
        from index_bot.mds_mapper import suggest_filename_from_original, generate_filename, DOCUMENT_TYPES
        from datetime import date as date_cls

        # Resolve classification
        if request.mds_code and request.doc_type:
            doc_date = date_cls.fromisoformat(request.doc_date) if request.doc_date else date_cls.today()
            new_name = generate_filename(
                mds_code=request.mds_code,
                doc_type=request.doc_type,
                doc_date=doc_date,
                version=request.version,
                original_filename=request.file_name,
            )
            mds_code = request.mds_code
            doc_type = request.doc_type
        else:
            suggestion = suggest_filename_from_original(request.file_name)
            new_name = suggestion["suggested_filename"]
            mds_code = suggestion["mds_code"]
            doc_type = suggestion["doc_type"]

        if request.dry_run:
            return JSONResponse({
                "dry_run": True,
                "file_id": request.file_id,
                "original_name": request.file_name,
                "proposed_name": new_name,
                "proposed_folder": f"/Camelot/{mds_code}/{doc_type}/" if mds_code else "UNRESOLVED",
            })

        try:
            from index_bot.drive_organizer import rename_file, move_to_correct_folder, move_to_unresolved

            # Rename
            rename_file(request.file_id, new_name)

            # Move
            if mds_code and doc_type:
                move_to_correct_folder(request.file_id, mds_code, doc_type)
                destination = f"/Camelot/{mds_code}/{doc_type}/"
            else:
                move_to_unresolved(request.file_id, reason=f"Cannot determine MDS code for: {request.file_name}")
                destination = "/Camelot/Incoming/UNRESOLVED/"

            return JSONResponse({
                "success": True,
                "file_id": request.file_id,
                "original_name": request.file_name,
                "new_name": new_name,
                "mds_code": mds_code,
                "doc_type": doc_type,
                "destination_folder": destination,
            })
        except Exception as e:
            logger.error(f"Rename/move error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # ---------------------------------------------------------------------------
    # POST /make/incoming — Make.com webhook endpoint
    # ---------------------------------------------------------------------------

    @app.post("/make/incoming", tags=["Make.com"])
    async def make_incoming_webhook(
        request: MakeWebhookRequest,
        x_webhook_secret: Optional[str] = Header(None, alias="X-Webhook-Secret"),
    ):
        """
        Full Make.com webhook handler.
        Triggered when a new file lands in /Camelot/Incoming/.
        Runs full pipeline: classify → rename → move → log.
        """
        # Validate secret
        if WEBHOOK_SECRET and x_webhook_secret != WEBHOOK_SECRET:
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

        from index_bot.make_webhook import receive_make_webhook, make_success_response
        payload = request.dict()
        result = receive_make_webhook(payload, secret_header=x_webhook_secret)
        response = make_success_response(result)

        status_code = 200 if result.get("success") else 207  # 207 = partial success
        return JSONResponse(content=response, status_code=status_code)

    # ---------------------------------------------------------------------------
    # GET /index/{mds_code}
    # ---------------------------------------------------------------------------

    @app.get("/index/{mds_code}", tags=["Index Reports"])
    async def get_building_index(
        mds_code: str,
        folder_id: Optional[str] = Query(None, description="Google Drive folder ID for this building"),
        format: str = Query("markdown", description="Output format: markdown | csv | both"),
    ):
        """
        Return the file index for a building by MDS code.
        Optionally specify a Drive folder_id; otherwise the bot looks it up.
        """
        from index_bot.drive_organizer import (
            get_camelot_root_id,
            find_folder,
            generate_index_report,
        )

        try:
            # Find the building folder if not provided
            if not folder_id:
                root_id = get_camelot_root_id()
                folder_id = find_folder(mds_code, root_id)
                if not folder_id:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Building folder for MDS code '{mds_code}' not found in Drive"
                    )

            report = generate_index_report(
                folder_id=folder_id,
                mds_code=mds_code,
                output_format=format,
            )

            if format == "csv":
                return PlainTextResponse(report["csv_content"], media_type="text/csv")
            elif format == "markdown":
                return PlainTextResponse(report["markdown_content"], media_type="text/markdown")
            else:
                return JSONResponse({
                    "mds_code": mds_code,
                    "file_count": report["file_count"],
                    "unindexed_count": report["unindexed_count"],
                    "doc_type_counts": report["doc_type_counts"],
                    "generated_at": report["generated_at"],
                    "csv": report["csv_content"],
                    "markdown": report["markdown_content"],
                })
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Index report error for {mds_code}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    # ---------------------------------------------------------------------------
    # GET /buildings
    # ---------------------------------------------------------------------------

    @app.get("/buildings", tags=["Index Reports"])
    async def list_buildings():
        """List all registered MDS building codes."""
        from index_bot.mds_mapper import list_all_buildings
        buildings = list_all_buildings()
        return JSONResponse({"buildings": buildings, "count": len(buildings)})

    # ---------------------------------------------------------------------------
    # GET /classify-batch (bulk filename classification)
    # ---------------------------------------------------------------------------

    @app.post("/classify-batch", tags=["Classification"])
    async def classify_batch(filenames: list[str], default_mds_code: Optional[str] = None):
        """
        Classify multiple filenames at once.
        Returns list of classification results.
        """
        from index_bot.mds_mapper import suggest_filename_from_original
        results = []
        for filename in filenames[:50]:  # Cap at 50 per request
            try:
                suggestion = suggest_filename_from_original(
                    filename, default_mds_code=default_mds_code
                )
                results.append(suggestion)
            except Exception as e:
                results.append({
                    "original_filename": filename,
                    "error": str(e),
                })
        return JSONResponse({"results": results, "count": len(results)})

    return app


# ---------------------------------------------------------------------------
# App instance (for uvicorn)
# ---------------------------------------------------------------------------

app = create_app() if _FASTAPI_AVAILABLE else None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    if not _FASTAPI_AVAILABLE:
        print("ERROR: FastAPI not installed. Run: pip install fastapi uvicorn pydantic")
        sys.exit(1)

    try:
        import uvicorn
    except ImportError:
        print("ERROR: uvicorn not installed. Run: pip install uvicorn")
        sys.exit(1)

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8002"))
    reload = os.getenv("ENVIRONMENT", "production") == "development"

    logger.info(f"Starting Index Bot API on {host}:{port}")
    uvicorn.run(
        "index_bot.api_server:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )
