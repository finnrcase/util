from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.inputs import WorkloadInput
from src.pipeline import run_util_pipeline


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_CARBON_PATH = DATA_DIR / "sample_carbon_forecast.csv"
DEFAULT_PRICE_PATH = DATA_DIR / "sample_price_forecast.csv"


def _extract_forecast_access_mode(result: dict[str, Any]) -> str | None:
    forecast_df = result.get("forecast")
    if isinstance(forecast_df, pd.DataFrame) and "forecast_access_mode" in forecast_df.columns:
        non_null_modes = forecast_df["forecast_access_mode"].dropna()
        if not non_null_modes.empty:
            return str(non_null_modes.iloc[0])
    return None


def run_multi_location_analysis(
    zip_codes: list[str],
    compute_hours_required: int,
    deadline,
    objective: str,
    machine_watts: int,
    mapping_path,
    forecast_mode: str,
    schedule_mode: str,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """
    Returns (summary_df, timeseries).

    summary_df    — one row per ZIP with scalar metrics (existing shape, unchanged).
    timeseries    — list of dicts, one per ZIP, each containing:
                      zip_code  : str
                      region    : str | None
                      location  : str  (display label: "ZIP · region")
                      data      : pd.DataFrame with timestamp, price_per_kwh,
                                  carbon_g_per_kwh, run_flag columns
    """
    rows: list[dict[str, Any]] = []
    timeseries: list[dict[str, Any]] = []

    for zip_code in zip_codes:
        workload = WorkloadInput(
            zip_code=zip_code,
            compute_hours_required=int(compute_hours_required),
            deadline=deadline,
            objective=objective,
            machine_watts=int(machine_watts),
        )

        result = run_util_pipeline(
            workload_input=workload,
            mapping_path=mapping_path,
            carbon_path=DEFAULT_CARBON_PATH,
            price_path=DEFAULT_PRICE_PATH,
            forecast_mode=forecast_mode,
            schedule_mode=schedule_mode,
        )

        metrics = result["metrics"]
        region = result["region"]
        rows.append(
            {
                "zip_code": zip_code,
                "region": region,
                "optimized_cost": metrics["optimized_cost"],
                "optimized_carbon_kg": metrics["optimized_carbon_kg"],
                "forecast_access_mode": _extract_forecast_access_mode(result),
            }
        )

        # Capture per-zip time series for overlay charts.
        # result["schedule"] already has timestamp, price_per_kwh,
        # carbon_g_per_kwh, and run_flag — no extra computation needed.
        schedule_df = result.get("schedule")
        if schedule_df is not None and not schedule_df.empty:
            ts_cols = [c for c in ["timestamp", "price_per_kwh", "carbon_g_per_kwh", "run_flag"]
                       if c in schedule_df.columns]
            ts = schedule_df[ts_cols].copy()
            label = f"{zip_code} · {region}" if region and region != zip_code else zip_code
            ts["location"] = label
            timeseries.append({
                "zip_code": zip_code,
                "region": region,
                "location": label,
                "data": ts,
            })

    return pd.DataFrame(rows), timeseries
