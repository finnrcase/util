from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class OptimizeRequest(BaseModel):
    zip_code: str = Field(..., min_length=5, max_length=5)
    compute_hours_required: int = Field(..., gt=0)
    deadline: datetime
    objective: Literal["carbon", "cost", "balanced"]
    machine_watts: int = Field(..., gt=0)
    carbon_weight: float = Field(0.5, ge=0.0, le=1.0)
    price_weight: float = Field(0.5, ge=0.0, le=1.0)
    forecast_mode: Literal["demo", "live_carbon"] = "live_carbon"
    schedule_mode: Literal["flexible", "block"] = "flexible"
    carbon_estimation_mode: Literal[
        "forecast_only",
        "forecast_plus_historical_expectation",
    ] = "forecast_only"
    historical_days: int = Field(7, ge=1, le=30)
    current_time_override: datetime | None = None
    include_diagnostics: bool = False


class ExportRequest(OptimizeRequest):
    export_root: str | None = None
    enable_cloud_upload: bool = False


class CoverageResponse(BaseModel):
    summary: str
    supported_live_markets: list[dict[str, Any]]
    partially_supported_notes: list[dict[str, Any]] = []
    unsupported_behavior: dict[str, Any]
    notes: list[str]


class ExportArtifactResponse(BaseModel):
    filename: str
    display_name: str
    artifact_type: str
    path: str
    reference_path: str
    size_bytes: int


class ExportResponse(BaseModel):
    export_dir: str
    run_id: str
    artifacts: list[ExportArtifactResponse]
    cloud_upload_enabled: bool
    cloud_message: str = ""
    summary: dict[str, Any] | None = None


class OptimizeResponse(BaseModel):
    input: dict[str, Any]
    location: dict[str, Any]
    pricing: dict[str, Any]
    forecast: dict[str, Any]
    summary: dict[str, Any]
    metrics: dict[str, Any]
    schedule: dict[str, Any]
    charts: dict[str, Any]
    provenance: dict[str, Any]
    diagnostics: dict[str, Any] | None = None
