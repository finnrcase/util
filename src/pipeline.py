"""
End-to-end pipeline for Util.

This module connects:
- validated user input
- ZIP-to-region mapping
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
- Region is mapped and returned, but live carbon is currently using a prototype flow
- Designed to make the backend app-ready before Streamlit integration
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from src.data_fetcher import get_forecast_table
from src.location.location_service import resolve_zip_to_watttime_region
from src.mapper import load_zip_region_map, map_zip_to_region


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

    Parameters
    ----------
    workload_input : WorkloadInput
        Validated user input object.
    mapping_path : str | Path
        Path to ZIP-to-region mapping CSV.
    carbon_path : str | Path | None
        Path to carbon forecast CSV (required for demo mode).
    price_path : str | Path | None
        Path to price forecast CSV (required for demo mode).
    forecast_mode : str
        Forecast loading mode.
        Supported values:
        - "demo"
        - "live_carbon"
    schedule_mode : str
        Schedule optimization mode.
        Supported values:
        - "flexible"
        - "block"
    carbon_estimation_mode : str
        Carbon estimate strategy for live mode.
        Supported values:
        - "forecast_only"
        - "forecast_plus_historical_expectation"
    historical_days : int
        Number of past days of historical carbon data to use when extending
        beyond the live forecast horizon.

    Returns
    -------
    dict
        Dictionary containing:
        - workload_input
        - region
        - forecast
        - baseline
        - optimized
        - schedule
        - metrics
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

    if forecast_mode == "demo":
        mapping_df = load_zip_region_map(mapping_path)
        region = map_zip_to_region(workload_input.zip_code, mapping_df)
        location_info = {
            "zip_code": workload_input.zip_code,
            "latitude": None,
            "longitude": None,
            "watttime_region": region,
            "watttime_name": None,
            "watttime_id": None,
        }

    elif forecast_mode == "live_carbon":
        location_info = resolve_zip_to_watttime_region(workload_input.zip_code)
        region = location_info["watttime_region"]

    else:
        raise ValueError(
            f"Unsupported forecast_mode: {forecast_mode}. "
            f"Expected 'demo' or 'live_carbon'."
        )

    forecast_df = get_forecast_table(
        forecast_mode=forecast_mode,
        region=region,
        carbon_filepath=carbon_path,
        price_filepath=price_path,
        carbon_estimation_mode=carbon_estimation_mode,
        historical_days=historical_days,
        deadline=workload_input.deadline,
    )

    baseline_df = build_baseline_func(
        forecast_df=forecast_df,
        compute_hours_required=workload_input.compute_hours_required,
    )

    optimized_df = optimize_func(
        forecast_df=forecast_df,
        compute_hours_required=workload_input.compute_hours_required,
        objective=workload_input.objective,
        deadline=workload_input.deadline,
        schedule_mode=schedule_mode,
        current_time_override=current_time_override,
    )

    schedule_df = build_schedule_func(optimized_df)

    metrics_dict = calculate_metrics_func(
        baseline_df=baseline_df,
        optimized_df=optimized_df,
        machine_watts=workload_input.machine_watts,
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