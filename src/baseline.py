"""
Baseline scheduling logic for Util.

The baseline represents a naive schedule:
start immediately and run continuously until the workload is complete.
"""

import math

import pandas as pd


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
    rows_per_hour = 60 / interval_minutes
    slots_required = math.ceil(compute_hours_required * rows_per_hour)

    if slots_required <= 0:
        raise ValueError("Computed slots_required must be positive")

    if current_time_override is not None:
        now_ts = pd.to_datetime(current_time_override)
        if getattr(now_ts, "tzinfo", None) is not None:
            now_ts = now_ts.tz_localize(None)
    else:
        now_ts = pd.Timestamp.now()

    # Mirror optimizer behavior for demo forecasts whose timestamps are
    # entirely in the past relative to the machine clock.
    if df["timestamp"].max() < now_ts:
        effective_now_ts = df["timestamp"].min()
    else:
        effective_now_ts = now_ts

    eligible_mask = df["timestamp"] >= effective_now_ts

    if deadline is not None:
        deadline_ts = pd.to_datetime(deadline)
        if getattr(deadline_ts, "tzinfo", None) is not None:
            deadline_ts = deadline_ts.tz_localize(None)
        eligible_mask = eligible_mask & (df["timestamp"] <= deadline_ts)

    eligible_df = df[eligible_mask].copy()

    if slots_required > len(eligible_df):
        raise ValueError(
            "compute_hours_required exceeds the amount of forecast time "
            "available between now and the deadline"
        )

    df["baseline_run_flag"] = 0
    eligible_indices = eligible_df.index[:slots_required]
    df.loc[eligible_indices, "baseline_run_flag"] = 1

    return df
