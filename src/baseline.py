"""
Baseline scheduling logic for Util.

The baseline represents a naive schedule:
start immediately and run continuously until the workload is complete.
"""

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
) -> pd.DataFrame:
    """
    Build a naive baseline schedule by selecting the earliest available intervals.

    Parameters
    ----------
    forecast_df : pd.DataFrame
        Forecast table containing timestamps and forecast values.
    compute_hours_required : int
        Number of compute hours required.

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
    slots_required = int(round(compute_hours_required * rows_per_hour))

    if slots_required <= 0:
        raise ValueError("Computed slots_required must be positive")

    if slots_required > len(df):
        raise ValueError("compute_hours_required exceeds available forecast horizon")

    df["baseline_run_flag"] = 0
    df.loc[: slots_required - 1, "baseline_run_flag"] = 1

    return df