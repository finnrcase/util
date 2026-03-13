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
- Uses placeholder forecast CSVs
- Region is mapped and returned, but placeholder forecasts are not yet region-specific
- Designed to make the backend app-ready before Streamlit integration
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from src.data_fetcher import build_forecast_table
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
    carbon_path: str | Path,
    price_path: str | Path,
) -> dict[str, Any]:
    """
    Run the full Util backend workflow.

    Parameters
    ----------
    workload_input : WorkloadInput
        Validated user input object.
    mapping_path : str | Path
        Path to ZIP-to-region mapping CSV.
    carbon_path : str | Path
        Path to carbon forecast CSV.
    price_path : str | Path
        Path to price forecast CSV.

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

    # 1. Map ZIP to region
    mapping_df = load_zip_region_map(mapping_path)
    region = map_zip_to_region(workload_input.zip_code, mapping_df)

    # 2. Load merged forecast table
    forecast_df = build_forecast_table(
        carbon_filepath=carbon_path,
        price_filepath=price_path,
    )

    # 3. Build naive baseline
    baseline_df = build_baseline_func(
        forecast_df=forecast_df,
        compute_hours_required=workload_input.compute_hours_required,
    )

    # 4. Run optimization
    optimized_df = optimize_func(
        forecast_df=forecast_df,
        compute_hours_required=workload_input.compute_hours_required,
        objective=workload_input.objective,
        deadline=workload_input.deadline,
    )

    # 5. Build readable schedule
    schedule_df = build_schedule_func(optimized_df)

    # 6. Calculate comparison metrics
    metrics_dict = calculate_metrics_func(
        baseline_df=baseline_df,
        optimized_df=optimized_df,
        machine_watts=workload_input.machine_watts,
    )

    return {
        "workload_input": workload_input,
        "region": region,
        "forecast": forecast_df,
        "baseline": baseline_df,
        "optimized": optimized_df,
        "schedule": schedule_df,
        "metrics": metrics_dict,
    }