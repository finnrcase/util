from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd


ANALYTICS_COLUMNS = [
    "timestamp",
    "run_type",
    "compute_hours",
    "region",
    "zip_code",
    "schedule_mode",
    "objective_mode",
    "machine_watts",
    "machine_kw",
    "baseline_emissions",
    "optimized_emissions",
    "carbon_saved",
    "carbon_reduction_pct",
    "baseline_cost",
    "optimized_cost",
    "cost_saved",
    "selected_interval_count",
    "eligible_interval_count",
    "best_start_time",
    "deadline",
    "api_mode",
    "forecast_mode",
    "notes",
]

NUMERIC_COLUMNS = [
    "compute_hours",
    "machine_watts",
    "machine_kw",
    "baseline_emissions",
    "optimized_emissions",
    "carbon_saved",
    "carbon_reduction_pct",
    "baseline_cost",
    "optimized_cost",
    "cost_saved",
    "selected_interval_count",
    "eligible_interval_count",
]


def analytics_file_exists(path: str | Path) -> bool:
    return Path(path).exists()


def _empty_analytics_df() -> pd.DataFrame:
    return pd.DataFrame(columns=ANALYTICS_COLUMNS)


def _normalize_analytics_df(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()

    for column in ANALYTICS_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA

    normalized = normalized[ANALYTICS_COLUMNS]

    if not normalized.empty:
        normalized["timestamp"] = pd.to_datetime(
            normalized["timestamp"], errors="coerce"
        )
        normalized["best_start_time"] = pd.to_datetime(
            normalized["best_start_time"], errors="coerce"
        )
        normalized["deadline"] = pd.to_datetime(
            normalized["deadline"], errors="coerce"
        )

        for column in NUMERIC_COLUMNS:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    return normalized


def load_analytics_data(path: str | Path) -> pd.DataFrame:
    analytics_path = Path(path)

    if not analytics_path.exists():
        return _empty_analytics_df()

    try:
        df = pd.read_csv(analytics_path)
    except pd.errors.EmptyDataError:
        return _empty_analytics_df()

    if df.empty:
        return _empty_analytics_df()

    return _normalize_analytics_df(df)


def append_run(path: str | Path, run_data: dict[str, Any]) -> None:
    analytics_path = Path(path)
    analytics_path.parent.mkdir(parents=True, exist_ok=True)

    current_df = load_analytics_data(analytics_path)

    row = {column: run_data.get(column) for column in ANALYTICS_COLUMNS}
    row_df = pd.DataFrame([row])
    updated_df = row_df if current_df.empty else pd.concat([current_df, row_df], ignore_index=True)
    normalized = _normalize_analytics_df(updated_df)
    normalized.to_csv(analytics_path, index=False)


def clear_analytics_data(path: str | Path) -> None:
    analytics_path = Path(path)
    analytics_path.parent.mkdir(parents=True, exist_ok=True)
    _empty_analytics_df().to_csv(analytics_path, index=False)


def filter_test_runs(
    df: pd.DataFrame,
    include_test_runs: bool = False,
) -> pd.DataFrame:
    if include_test_runs or df.empty or "run_type" not in df.columns:
        return df.copy()

    return df[df["run_type"].fillna("Real").str.lower() != "test"].copy()


def filter_analytics_data(
    df: pd.DataFrame,
    include_test_runs: bool = False,
    start_date: date | None = None,
    end_date: date | None = None,
    region: str | None = None,
    schedule_mode: str | None = None,
) -> pd.DataFrame:
    filtered = filter_test_runs(df, include_test_runs=include_test_runs)

    if filtered.empty:
        return filtered

    if start_date is not None:
        filtered = filtered[filtered["timestamp"].dt.date >= start_date]

    if end_date is not None:
        filtered = filtered[filtered["timestamp"].dt.date <= end_date]

    if region and region != "All":
        filtered = filtered[filtered["region"] == region]

    if schedule_mode and schedule_mode != "All":
        filtered = filtered[filtered["schedule_mode"] == schedule_mode]

    return filtered.copy()


def summarize_analytics(df: pd.DataFrame) -> dict[str, float | int | None]:
    total_logged_runs = int(len(df))
    total_real_runs = int(
        len(df[df["run_type"].fillna("Real").str.lower() == "real"])
    ) if not df.empty else 0
    total_test_runs = int(
        len(df[df["run_type"].fillna("").str.lower() == "test"])
    ) if not df.empty else 0

    real_runs_df = df[df["run_type"].fillna("Real").str.lower() == "real"].copy()

    total_compute_hours = float(df["compute_hours"].fillna(0).sum()) if "compute_hours" in df else 0.0
    total_carbon_saved = float(df["carbon_saved"].fillna(0).sum()) if "carbon_saved" in df else 0.0
    total_cost_saved = float(df["cost_saved"].fillna(0).sum()) if "cost_saved" in df else 0.0

    avg_carbon_saved_per_real_run = (
        float(real_runs_df["carbon_saved"].dropna().mean())
        if not real_runs_df.empty and not real_runs_df["carbon_saved"].dropna().empty
        else 0.0
    )
    avg_cost_saved_per_real_run = (
        float(real_runs_df["cost_saved"].dropna().mean())
        if not real_runs_df.empty and not real_runs_df["cost_saved"].dropna().empty
        else 0.0
    )
    avg_carbon_reduction_pct = (
        float(df["carbon_reduction_pct"].dropna().mean())
        if not df.empty and not df["carbon_reduction_pct"].dropna().empty
        else 0.0
    )
    avg_selected_interval_count = (
        float(df["selected_interval_count"].dropna().mean())
        if not df.empty and not df["selected_interval_count"].dropna().empty
        else 0.0
    )

    return {
        "total_logged_runs": total_logged_runs,
        "total_real_runs": total_real_runs,
        "total_test_runs": total_test_runs,
        "total_compute_hours": total_compute_hours,
        "total_carbon_saved": total_carbon_saved,
        "total_cost_saved": total_cost_saved,
        "avg_carbon_saved_per_real_run": avg_carbon_saved_per_real_run,
        "avg_cost_saved_per_real_run": avg_cost_saved_per_real_run,
        "avg_carbon_reduction_pct": avg_carbon_reduction_pct,
        "avg_selected_interval_count": avg_selected_interval_count,
    }
