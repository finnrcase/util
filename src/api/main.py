from __future__ import annotations

import logging
import os
import time
import traceback
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from src.api.schemas import CoverageResponse, ExportRequest, ExportResponse, OptimizeRequest, OptimizeResponse
from src.runtime_config import get_app_storage_root


MODULE_IMPORT_STARTED_AT = time.perf_counter()
PROCESS_STARTED_AT = float(os.environ.get("UTIL_PROCESS_STARTED_AT", str(time.time())))
api_logger = logging.getLogger("uvicorn.error")
ALLOWED_ORIGINS = [
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://localhost:3000",
    "http://tauri.localhost",
    "https://tauri.localhost",
    "tauri://localhost",
    "https://util-ten-delta.vercel.app",
]
ALLOWED_METHODS = ["GET", "POST", "OPTIONS"]
ALLOWED_HEADERS = ["Accept", "Content-Type", "Origin", "Authorization"]
EXPORTS_ROOT = (get_app_storage_root() / "exports" / "api").resolve()


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


@app.on_event("startup")
def log_app_startup() -> None:
    api_logger.info(
        "Util API startup complete: module_import_ms=%.1f process_to_startup_ms=%.1f",
        (time.perf_counter() - MODULE_IMPORT_STARTED_AT) * 1000.0,
        (time.time() - PROCESS_STARTED_AT) * 1000.0,
    )


@app.middleware("http")
async def log_optimize_requests(request: Request, call_next):
    started_at = time.perf_counter()
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
            "Util API optimize response: method=%s status=%s allow-origin=%s allow-methods=%s allow-headers=%s elapsed_ms=%.1f",
            request.method,
            response.status_code,
            response.headers.get("access-control-allow-origin", ""),
            response.headers.get("access-control-allow-methods", ""),
            response.headers.get("access-control-allow-headers", ""),
            (time.perf_counter() - started_at) * 1000.0,
        )
    return response


@app.post("/api/v1/optimize", response_model=OptimizeResponse)
def optimize(request: OptimizeRequest) -> OptimizeResponse:
    started_at = time.perf_counter()
    api_logger.info(
        "Util API optimize route parsed request: zip=%s objective=%s machine_watts=%s include_diagnostics=%s",
        request.zip_code,
        request.objective,
        request.machine_watts,
        request.include_diagnostics,
    )
    try:
        from src.api.service import build_optimize_response, execute_optimization

        result = execute_optimization(request)
        response = build_optimize_response(request, result)
        api_logger.info(
            "Util API optimize route success zip=%s objective=%s elapsed_ms=%.1f",
            request.zip_code,
            request.objective,
            (time.perf_counter() - started_at) * 1000.0,
        )
        return response
    except Exception as exc:
        trace = traceback.format_exc()
        api_logger.exception(
            "Util API optimize route failed zip=%s objective=%s elapsed_ms=%.1f",
            request.zip_code,
            request.objective,
            (time.perf_counter() - started_at) * 1000.0,
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": "Optimize request failed",
                "error_type": type(exc).__name__,
                "detail": str(exc),
                "traceback": trace,
            },
        )


@app.get("/health")
@app.get("/api/v1/health")
def health() -> dict[str, str]:
    api_logger.info(
        "Util API health ready: process_uptime_ms=%.1f",
        (time.time() - PROCESS_STARTED_AT) * 1000.0,
    )
    return {
        "status": "ok",
        "service": "util-api",
    }


@app.get("/api/v1/coverage", response_model=CoverageResponse)
def coverage() -> CoverageResponse:
    from src.api.service import build_coverage_response

    return build_coverage_response()


@app.post("/api/v1/export", response_model=ExportResponse)
def export(request: ExportRequest) -> ExportResponse:
    from src.api.service import build_export_response

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
