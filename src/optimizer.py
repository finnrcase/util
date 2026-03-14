"""
Optimization logic for Util.

DEVELOPMENT NOTE:
This version supports deadline-aware optimization on placeholder data.
It selects the best available time slots up to the specified deadline
based on the chosen objective.

TODO:
Later replace/extend with more advanced constrained optimization,
partial-load scheduling, and finer time intervals.
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


def optimize_schedule(
    forecast_df: pd.DataFrame,
    compute_hours_required: int,
    objective: str,
    deadline: str | None = None,
) -> pd.DataFrame:
    """
    Select the best times to run based on the chosen objective,
    constrained to the window between now and the specified deadline.

    Parameters
    ----------
    forecast_df : pd.DataFrame
        Must contain:
        - timestamp
        - carbon_g_per_kwh
        - price_per_kwh
    compute_hours_required : int
        Number of compute hours needed.
    objective : str
        Either 'carbon' or 'cost'.
    deadline : str | None
        Optional ISO-format datetime string. Only rows between the current
        time and this deadline will be eligible for selection.

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

    required_columns = {"timestamp", "carbon_g_per_kwh", "price_per_kwh"}
    if not required_columns.issubset(forecast_df.columns):
        raise ValueError(
            f"forecast_df must contain columns: {required_columns}"
        )

    df = forecast_df.copy()
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Infer interval length from forecast data
    interval_minutes = _infer_interval_minutes(df)
    rows_per_hour = 60 / interval_minutes
    slots_required = int(round(compute_hours_required * rows_per_hour))

    if slots_required <= 0:
        raise ValueError("Computed slots_required must be positive.")

    # Define eligibility window: current time through deadline
    now_ts = pd.Timestamp.now()

    df["eligible_flag"] = (df["timestamp"] >= now_ts).astype(int)

    if deadline is not None:
        deadline_ts = pd.to_datetime(deadline)
        df["eligible_flag"] = (
            (df["timestamp"] >= now_ts) & (df["timestamp"] <= deadline_ts)
        ).astype(int)

    eligible_df = df[df["eligible_flag"] == 1].copy()

    if slots_required > len(eligible_df):
        raise ValueError(
            "compute_hours_required exceeds the amount of forecast time "
            "available between now and the deadline"
        )

    if objective == "carbon":
        eligible_df["score"] = eligible_df["carbon_g_per_kwh"]
    else:
        eligible_df["score"] = eligible_df["price_per_kwh"]

    eligible_df = eligible_df.sort_values("score", ascending=True).reset_index(drop=True)
    eligible_df["run_flag"] = 0
    eligible_df.loc[: slots_required - 1, "run_flag"] = 1

    df = df.merge(
        eligible_df[["timestamp", "score", "run_flag"]],
        on="timestamp",
        how="left",
    )

    df["run_flag"] = df["run_flag"].fillna(0).astype(int)

    return df.sort_values("timestamp").reset_index(drop=True)