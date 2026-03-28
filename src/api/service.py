from __future__ import annotations

from pathlib import Path
import logging

from src.api.schemas import CoverageResponse, ExportRequest, ExportResponse, OptimizeRequest, OptimizeResponse
from src.api.serializers import (
    build_chart_payloads,
    build_diagnostics_summary,
    build_export_artifact_list,
    build_forecast_summary,
    build_input_summary,
    build_location_summary,
    build_metrics_summary,
    build_pricing_summary,
    build_provenance_summary,
    build_schedule_summary,
    build_summary,
)
from src.exporter import generate_export_package
from src.inputs import WorkloadInput
from src.pipeline import run_util_pipeline


api_logger = logging.getLogger("uvicorn.error")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_MAPPING_PATH = DATA_DIR / "zip_to_region_sample.csv"
DEFAULT_CARBON_PATH = DATA_DIR / "sample_carbon_forecast.csv"
DEFAULT_PRICE_PATH = DATA_DIR / "sample_price_forecast.csv"
DEFAULT_EXPORT_ROOT = PROJECT_ROOT / "exports" / "api"


def execute_optimization(request: OptimizeRequest) -> dict:
    api_logger.info(
        "Util API optimize engine start: zip=%s objective=%s hours=%s deadline=%s forecast_mode=%s schedule_mode=%s",
        request.zip_code,
        request.objective,
        request.compute_hours_required,
        request.deadline,
        request.forecast_mode,
        request.schedule_mode,
    )

    workload = WorkloadInput(
        zip_code=request.zip_code,
        compute_hours_required=request.compute_hours_required,
        deadline=request.deadline,
        objective=request.objective,
        machine_watts=request.machine_watts,
        carbon_weight=request.carbon_weight,
        price_weight=request.price_weight,
    )

    result = run_util_pipeline(
        workload_input=workload,
        mapping_path=DEFAULT_MAPPING_PATH,
        carbon_path=DEFAULT_CARBON_PATH,
        price_path=DEFAULT_PRICE_PATH,
        forecast_mode=request.forecast_mode,
        schedule_mode=request.schedule_mode,
        carbon_estimation_mode=request.carbon_estimation_mode,
        historical_days=request.historical_days,
        current_time_override=request.current_time_override.isoformat() if request.current_time_override else None,
    )

    api_logger.info(
        "Util API optimize engine success: region=%s forecast_rows=%s optimized_rows=%s schedule_rows=%s",
        result.get("region"),
        len(result.get("forecast", [])),
        len(result.get("optimized", [])),
        len(result.get("schedule", [])),
    )
    return result


def build_optimize_response(request: OptimizeRequest, result: dict) -> OptimizeResponse:
    api_logger.info("Util API optimize response build start")
    diagnostics = build_diagnostics_summary(result) if request.include_diagnostics else None
    response = OptimizeResponse(
        input=build_input_summary(request.model_dump()),
        location=build_location_summary(result),
        pricing=build_pricing_summary(result),
        forecast=build_forecast_summary(result),
        summary=build_summary(result),
        metrics=build_metrics_summary(result),
        schedule=build_schedule_summary(result),
        charts=build_chart_payloads(result),
        provenance=build_provenance_summary(result),
        diagnostics=diagnostics,
    )
    api_logger.info(
        "Util API optimize response build success: summary_cards=%s badges=%s chart_keys=%s",
        len(response.summary.get("cards", [])),
        len(response.summary.get("badges", [])),
        sorted(response.charts.keys()),
    )
    return response


def build_coverage_response() -> CoverageResponse:
    return CoverageResponse(
        summary="Util currently supports live location-aware pricing for CAISO-routed California regions and the ERCOT Houston route. Other markets remain available with clearly labeled fallback pricing.",
        supported_live_markets=[
            {
                "market": "CAISO",
                "coverage": "California regions routed through CAISO Day-Ahead pricing",
                "examples": ["LDWP", "CAISO_NORTH", "SCE", "SDGE", "PGE"],
                "status": "supported",
            },
            {
                "market": "ERCOT",
                "coverage": "Houston route using ERCOT East Texas to LZ_HOUSTON Day-Ahead pricing",
                "examples": ["ERCOT_EASTTX"],
                "status": "supported",
            },
        ],
        partially_supported_notes=[
            {
                "market": "ERCOT",
                "note": "Coverage currently targets the Houston route through ERCOT_EASTTX -> LZ_HOUSTON.",
            }
        ],
        unsupported_behavior={
            "status": "fallback_pricing",
            "label": "Fallback pricing",
            "message": "If a region does not have a live pricing route yet, Util completes the run with clearly labeled fallback pricing instead of pretending live market coverage exists.",
        },
        notes=[
            "Live location lookup still runs through WattTime for supported and unsupported regions.",
            "PJM and NYISO are not part of this thin API phase.",
        ],
    )


def build_export_response(request: ExportRequest) -> ExportResponse:
    result = execute_optimization(request)
    export_package = generate_export_package(
        result=result,
        export_root=request.export_root or str(DEFAULT_EXPORT_ROOT),
        enable_cloud_upload=request.enable_cloud_upload,
    )
    return ExportResponse(
        export_dir=str(export_package["export_dir"]),
        run_id=str(export_package["run_id"]),
        artifacts=build_export_artifact_list(export_package),
        cloud_upload_enabled=bool(export_package.get("cloud_save_enabled")),
        cloud_message=str(export_package.get("cloud_message", "") or ""),
        summary={
            "artifact_count": len(export_package.get("files", [])),
            "includes_provenance_summary": True,
            "export_type": "csv_package",
        },
    )
