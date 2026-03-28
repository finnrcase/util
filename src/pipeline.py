"""
End-to-end pipeline for Util.

This module connects:
- validated user input
- location / region resolution
- forecast loading
- baseline construction
- optimization
- schedule formatting
- metrics calculation

Current development status:
- Supports demo forecast mode using placeholder CSVs
- Supports live carbon mode using WattTime + placeholder price
- Supports flexible and block schedule optimization modes
- Supports forecast-only and forecast-extension carbon estimation modes
- Uses CSV ZIP-to-region mapping in demo mode
- Uses live ZIP -> coordinates -> WattTime region lookup in live carbon mode
- Designed to make the backend app-ready before Streamlit integration
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Callable

from src.data_fetcher import get_forecast_table
from src.location.location_service import resolve_zip_to_watttime_region
from src.mapper import load_zip_region_map, map_zip_to_region
from src.scheduling_window import get_current_reference_time


pipeline_logger = logging.getLogger("uvicorn.error")


def _resolve_callable(module: Any, candidate_names: list[str]) -> Callable:
    """
    Find the first matching callable in a module from a list of possible names.

    This makes the pipeline more robust while the codebase is still evolving.
    """
    for name in candidate_names:
        func = getattr(module, name, None)
        if callable(func):
            return func

    raise AttributeError(
        f"Could not find any of these functions in {module.__name__}: {candidate_names}"
    )


def run_util_pipeline(
    workload_input: Any,
    mapping_path: str | Path,
    carbon_path: str | Path | None = None,
    price_path: str | Path | None = None,
    forecast_mode: str = "demo",
    schedule_mode: str = "flexible",
    carbon_estimation_mode: str = "forecast_only",
    historical_days: int = 7,
    current_time_override: str | None = None,
) -> dict[str, Any]:
    """
    Run the full Util backend workflow.
    """
    from src import baseline, metrics, optimizer, scheduler

    build_baseline_func = _resolve_callable(
        baseline,
        [
            "build_baseline_schedule",
            "create_baseline_schedule",
            "build_baseline",
            "create_baseline",
        ],
    )

    optimize_func = _resolve_callable(
        optimizer,
        [
            "optimize_schedule",
            "run_optimizer",
            "build_optimized_schedule",
            "optimize",
        ],
    )

    build_schedule_func = _resolve_callable(
        scheduler,
        [
            "format_schedule",
            "build_schedule_table",
            "create_schedule_table",
            "build_schedule",
        ],
    )

    calculate_metrics_func = _resolve_callable(
        metrics,
        [
            "compare_schedules",
            "calculate_metrics",
            "compute_metrics",
            "compare_schedule_metrics",
            "calculate_schedule_metrics",
        ],
    )

    total_started_at = time.perf_counter()
    pipeline_logger.info(
        "Util pipeline: start zip=%s forecast_mode=%s schedule_mode=%s carbon_mode=%s historical_days=%s",
        getattr(workload_input, "zip_code", ""),
        forecast_mode,
        schedule_mode,
        carbon_estimation_mode,
        historical_days,
    )

    if forecast_mode == "demo":
        mapping_started_at = time.perf_counter()
        pipeline_logger.info("Util pipeline: demo mode mapping start zip=%s", workload_input.zip_code)
        mapping_df = load_zip_region_map(mapping_path)
        region = map_zip_to_region(workload_input.zip_code, mapping_df)
        pipeline_logger.info(
            "Util pipeline: demo mode mapping success zip=%s region=%s elapsed_ms=%.1f",
            workload_input.zip_code,
            region,
            (time.perf_counter() - mapping_started_at) * 1000.0,
        )

        location_info = {
            "zip_code": workload_input.zip_code,
            "latitude": None,
            "longitude": None,
            "watttime_region": region,
            "watttime_region_full_name": None,
            "signal_type_used": None,
            "location_lookup_status": "demo_mapping",
            "raw_response": None,
        }

    elif forecast_mode == "live_carbon":
        location_started_at = time.perf_counter()
        pipeline_logger.info("Util pipeline: live location resolution start zip=%s", workload_input.zip_code)
        location_info = resolve_zip_to_watttime_region(workload_input.zip_code)
        region = location_info["watttime_region"]
        pipeline_logger.info(
            "Util pipeline: live location resolution success zip=%s lat=%s lon=%s region=%s elapsed_ms=%.1f",
            workload_input.zip_code,
            location_info["latitude"],
            location_info["longitude"],
            region,
            (time.perf_counter() - location_started_at) * 1000.0,
        )

    else:
        raise ValueError(
            f"Unsupported forecast_mode: {forecast_mode}. "
            f"Expected 'demo' or 'live_carbon'."
        )

    reference_now = get_current_reference_time(current_time_override)

    forecast_started_at = time.perf_counter()
    pipeline_logger.info("Util pipeline: forecast fetch start region=%s", region)
    forecast_df = get_forecast_table(
        forecast_mode=forecast_mode,
        region=region,
        carbon_filepath=carbon_path,
        price_filepath=price_path,
        carbon_estimation_mode=carbon_estimation_mode,
        historical_days=historical_days,
        deadline=workload_input.deadline,
    )
    pipeline_logger.info(
        "Util pipeline: forecast fetch success region=%s rows=%s elapsed_ms=%.1f",
        region,
        len(forecast_df),
        (time.perf_counter() - forecast_started_at) * 1000.0,
    )

    print(
        "[OPTIMIZER INPUT DEBUG] Pricing context:",
        {
            "pricing_status": (
                forecast_df["pricing_status"].dropna().iloc[0]
                if "pricing_status" in forecast_df.columns and not forecast_df["pricing_status"].dropna().empty
                else ""
            ),
            "pricing_source": (
                forecast_df["pricing_source"].dropna().iloc[0]
                if "pricing_source" in forecast_df.columns and not forecast_df["pricing_source"].dropna().empty
                else ""
            ),
            "pricing_market": (
                forecast_df["pricing_market"].dropna().iloc[0]
                if "pricing_market" in forecast_df.columns and not forecast_df["pricing_market"].dropna().empty
                else ""
            ),
            "pricing_node": (
                forecast_df["pricing_node"].dropna().iloc[0]
                if "pricing_node" in forecast_df.columns and not forecast_df["pricing_node"].dropna().empty
                else ""
            ),
            "forecast_rows": len(forecast_df),
            "optimizer_intervals": len(forecast_df),
            "non_null_price_rows": int(forecast_df["price_per_kwh"].notna().sum()) if "price_per_kwh" in forecast_df.columns else 0,
            "placeholder_price_rows": int((forecast_df.get("price_signal_source") == "placeholder").sum()) if "price_signal_source" in forecast_df.columns else 0,
            "live_price_rows": int((forecast_df.get("price_signal_source") == "live_forecast").sum()) if "price_signal_source" in forecast_df.columns else 0,
        },
    )

    baseline_started_at = time.perf_counter()
    pipeline_logger.info("Util pipeline: baseline build start")
    baseline_df = build_baseline_func(
        forecast_df=forecast_df,
        compute_hours_required=workload_input.compute_hours_required,
        deadline=workload_input.deadline,
        current_time_override=reference_now,
    )
    pipeline_logger.info(
        "Util pipeline: baseline build success rows=%s elapsed_ms=%.1f",
        len(baseline_df),
        (time.perf_counter() - baseline_started_at) * 1000.0,
    )

    optimizer_started_at = time.perf_counter()
    pipeline_logger.info("Util pipeline: optimizer solve start objective=%s", workload_input.objective)
    optimized_df = optimize_func(
        forecast_df=forecast_df,
        compute_hours_required=workload_input.compute_hours_required,
        objective=workload_input.objective,
        deadline=workload_input.deadline,
        schedule_mode=schedule_mode,
        current_time_override=reference_now,
        carbon_weight=getattr(workload_input, "carbon_weight", 0.5),
        price_weight=getattr(workload_input, "price_weight", 0.5),
    )
    pipeline_logger.info(
        "Util pipeline: optimizer solve success rows=%s elapsed_ms=%.1f",
        len(optimized_df),
        (time.perf_counter() - optimizer_started_at) * 1000.0,
    )

    schedule_started_at = time.perf_counter()
    pipeline_logger.info("Util pipeline: schedule formatting start")
    schedule_df = build_schedule_func(optimized_df)
    pipeline_logger.info(
        "Util pipeline: schedule formatting success rows=%s elapsed_ms=%.1f",
        len(schedule_df),
        (time.perf_counter() - schedule_started_at) * 1000.0,
    )

    metrics_started_at = time.perf_counter()
    pipeline_logger.info("Util pipeline: metrics calculation start")
    metrics_dict = calculate_metrics_func(
        baseline_df=baseline_df,
        optimized_df=optimized_df,
        machine_watts=workload_input.machine_watts,
    )
    pipeline_logger.info(
        "Util pipeline: metrics calculation success elapsed_ms=%.1f total_elapsed_ms=%.1f",
        (time.perf_counter() - metrics_started_at) * 1000.0,
        (time.perf_counter() - total_started_at) * 1000.0,
    )

    return {
        "workload_input": workload_input,
        "region": region,
        "location_info": location_info,
        "forecast": forecast_df,
        "baseline": baseline_df,
        "optimized": optimized_df,
        "schedule": schedule_df,
        "metrics": metrics_dict,
    }
