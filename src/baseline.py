"""
Baseline scheduling logic for Util.

The baseline represents a naive schedule:
start immediately and run continuously until the workload is complete.
"""

import pandas as pd


def build_baseline_schedule(
    forecast_df: pd.DataFrame,
    compute_hours_required: int,
) -> pd.DataFrame:
    """
    Build a naive baseline schedule by selecting the earliest available hours.

    Parameters
    ----------
    forecast_df : pd.DataFrame
        Forecast table containing hourly timestamps and forecast values.
    compute_hours_required : int
        Number of hourly slots required.

    Returns
    -------
    pd.DataFrame
        Original forecast table with:
        - baseline_run_flag
    """
    if compute_hours_required <= 0:
        raise ValueError("compute_hours_required must be positive")

    if compute_hours_required > len(forecast_df):
        raise ValueError("compute_hours_required exceeds available forecast hours")

    df = forecast_df.copy()
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["baseline_run_flag"] = 0
    df.loc[: compute_hours_required - 1, "baseline_run_flag"] = 1

    return df