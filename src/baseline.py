"""
Baseline scheduling logic for Util.

The baseline represents a naive schedule:
start immediately and run continuously until the workload is complete.
"""

import pandas as pd

from src.scheduling_window import (
    build_eligibility_mask,
    calculate_required_slots,
    ensure_window_feasibility,
)


def _infer_interval_minutes(df: pd.DataFrame) -> float:
    """
    Infer the time interval in minutes between forecast rows.
    Assumes timestamp column is sorted.
    """
    if len(df) < 2:
        raise ValueError("forecast_df must contain at least 2 rows to infer interval.")

    diffs = df["timestamp"].sort_values().diff().dropna()
    interval_minutes = diffs.dt.total_seconds().median() / 60

    if interval_minutes <= 0:
        raise ValueError("Could not infer a valid forecast interval.")

    return interval_minutes


def build_baseline_schedule(
    forecast_df: pd.DataFrame,
    compute_hours_required: int,
    deadline: str | None = None,
    current_time_override: str | None = None,
) -> pd.DataFrame:
    """
    Build a naive baseline schedule by selecting the earliest available intervals.

    Parameters
    ----------
    forecast_df : pd.DataFrame
        Forecast table containing timestamps and forecast values.
    compute_hours_required : int
        Number of compute hours required.
    deadline : str | None
        Optional ISO-format datetime string. Only rows between the current
        time and this deadline will be eligible for baseline selection.
    current_time_override : str | None
        Optional override for baseline "now" timestamp, used for testing.

    Returns
    -------
    pd.DataFrame
        Original forecast table with:
        - baseline_run_flag
    """
    if compute_hours_required <= 0:
        raise ValueError("compute_hours_required must be positive")

    df = forecast_df.copy()
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    interval_minutes = _infer_interval_minutes(df)
    slots_required = calculate_required_slots(compute_hours_required, interval_minutes)

    if slots_required <= 0:
        raise ValueError("Computed slots_required must be positive")

    eligible_mask, _, _ = build_eligibility_mask(
        timestamps=df["timestamp"],
        deadline=deadline,
        current_time_override=current_time_override,
    )

    eligible_df = df[eligible_mask].copy()

    ensure_window_feasibility(slots_required, len(eligible_df))

    df["baseline_run_flag"] = 0
    eligible_indices = eligible_df.index[:slots_required]
    df.loc[eligible_indices, "baseline_run_flag"] = 1

    return df
