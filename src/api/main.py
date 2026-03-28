from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from src.api.schemas import CoverageResponse, ExportRequest, ExportResponse, OptimizeRequest, OptimizeResponse
from src.api.service import (
    PROJECT_ROOT,
    build_coverage_response,
    build_export_response,
    build_optimize_response,
    execute_optimization,
)


ALLOWED_ORIGINS = [
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://localhost:3000",
]
ALLOWED_METHODS = ["GET", "POST", "OPTIONS"]
ALLOWED_HEADERS = ["Accept", "Content-Type", "Origin", "Authorization"]
api_logger = logging.getLogger("uvicorn.error")
EXPORTS_ROOT = (PROJECT_ROOT / "exports").resolve()


app = FastAPI(
    title="Util API",
    version="0.1.0",
    description="Thin API boundary over the existing Util optimization engine.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=ALLOWED_METHODS,
    allow_headers=ALLOWED_HEADERS,
)


@app.middleware("http")
async def log_optimize_requests(request: Request, call_next):
    if request.url.path == "/api/v1/optimize":
        api_logger.info(
            "Util API optimize request: method=%s origin=%s acr-method=%s acr-headers=%s",
            request.method,
            request.headers.get("origin", ""),
            request.headers.get("access-control-request-method", ""),
            request.headers.get("access-control-request-headers", ""),
        )
    response = await call_next(request)
    if request.url.path == "/api/v1/optimize":
        api_logger.info(
            "Util API optimize response: method=%s status=%s allow-origin=%s allow-methods=%s allow-headers=%s",
            request.method,
            response.status_code,
            response.headers.get("access-control-allow-origin", ""),
            response.headers.get("access-control-allow-methods", ""),
            response.headers.get("access-control-allow-headers", ""),
        )
    return response


@app.post("/api/v1/optimize", response_model=OptimizeResponse)
def optimize(request: OptimizeRequest) -> OptimizeResponse:
    api_logger.info(
        "Util API optimize route parsed request: zip=%s objective=%s machine_watts=%s include_diagnostics=%s",
        request.zip_code,
        request.objective,
        request.machine_watts,
        request.include_diagnostics,
    )
    try:
        result = execute_optimization(request)
        return build_optimize_response(request, result)
    except Exception:
        api_logger.exception("Util API optimize route failed")
        raise


@app.get("/api/v1/coverage", response_model=CoverageResponse)
def coverage() -> CoverageResponse:
    return build_coverage_response()


@app.post("/api/v1/export", response_model=ExportResponse)
def export(request: ExportRequest) -> ExportResponse:
    return build_export_response(request)


@app.get("/api/v1/export/download")
def download_export(path: str = Query(..., min_length=1)) -> FileResponse:
    resolved_path = Path(path).expanduser().resolve()

    try:
        resolved_path.relative_to(EXPORTS_ROOT)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Requested export file is outside the allowed export directory.") from exc

    if not resolved_path.exists() or not resolved_path.is_file():
        raise HTTPException(status_code=404, detail="Requested export file was not found.")

    api_logger.info("Util API export download: path=%s", resolved_path)
    return FileResponse(path=resolved_path, filename=resolved_path.name, media_type="text/csv")
