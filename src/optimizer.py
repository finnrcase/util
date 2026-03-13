"""
Optimization logic for Util.

DEVELOPMENT NOTE:
This version supports deadline-aware optimization on placeholder data.
It selects the best available hours up to the specified deadline
based on the chosen objective.

TODO:
Later replace/extend with more advanced constrained optimization,
partial-load scheduling, and finer time intervals.
"""

import pandas as pd


def optimize_schedule(
    forecast_df: pd.DataFrame,
    compute_hours_required: int,
    objective: str,
    deadline: str | None = None,
) -> pd.DataFrame:
    """
    Select the best hours to run based on the chosen objective,
    optionally constrained by a deadline.

    Parameters
    ----------
    forecast_df : pd.DataFrame
        Must contain:
        - timestamp
        - carbon_g_per_kwh
        - price_per_kwh
    compute_hours_required : int
        Number of hourly slots needed.
    objective : str
        Either 'carbon' or 'cost'.
    deadline : str | None
        Optional ISO-format datetime string. Only hours at or before
        this deadline will be eligible for selection.

    Returns
    -------
    pd.DataFrame
        Original forecast table with:
        - score
        - eligible_flag
        - run_flag
    """
    if objective not in ["carbon", "cost"]:
        raise ValueError("objective must be either 'carbon' or 'cost'")

    if compute_hours_required <= 0:
        raise ValueError("compute_hours_required must be positive")

    df = forecast_df.copy()
    df = df.sort_values("timestamp").reset_index(drop=True)

    df["eligible_flag"] = 1

    if deadline is not None:
        deadline_ts = pd.to_datetime(deadline)
        df["eligible_flag"] = (df["timestamp"] <= deadline_ts).astype(int)

    eligible_df = df[df["eligible_flag"] == 1].copy()

    if compute_hours_required > len(eligible_df):
        raise ValueError(
            "compute_hours_required exceeds the number of hours available "
            "before the deadline"
        )

    if objective == "carbon":
        eligible_df["score"] = eligible_df["carbon_g_per_kwh"]
    else:
        eligible_df["score"] = eligible_df["price_per_kwh"]

    eligible_df = eligible_df.sort_values("score", ascending=True).reset_index(drop=True)
    eligible_df["run_flag"] = 0
    eligible_df.loc[: compute_hours_required - 1, "run_flag"] = 1

    df = df.merge(
        eligible_df[["timestamp", "score", "run_flag"]],
        on="timestamp",
        how="left",
    )

    df["run_flag"] = df["run_flag"].fillna(0).astype(int)

    return df.sort_values("timestamp").reset_index(drop=True)