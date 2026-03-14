"""
Optimization logic for Util.

DEVELOPMENT NOTE:
This version supports deadline-aware optimization and multiple schedule modes.

Supported schedule modes:
- flexible: choose the best individual eligible intervals
- block: choose one continuous run window

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


def _build_score_column(df: pd.DataFrame, objective: str) -> pd.DataFrame:
    """
    Add the optimization score column based on the selected objective.
    Lower score is better.
    """
    df = df.copy()

    if objective == "carbon":
        df["score"] = df["carbon_g_per_kwh"]
    elif objective == "cost":
        df["score"] = df["price_per_kwh"]
    else:
        raise ValueError("objective must be either 'carbon' or 'cost'")

    return df


def _select_flexible_schedule(
    eligible_df: pd.DataFrame,
    slots_required: int,
) -> pd.DataFrame:
    """
    Select the lowest-score individual eligible intervals.
    """
    selected_df = eligible_df.sort_values("score", ascending=True).copy()
    selected_df["run_flag"] = 0
    selected_df.iloc[:slots_required, selected_df.columns.get_loc("run_flag")] = 1
    return selected_df


def _select_block_schedule(
    eligible_df: pd.DataFrame,
    slots_required: int,
) -> pd.DataFrame:
    """
    Select one contiguous run window with the lowest total score.
    """
    if slots_required > len(eligible_df):
        raise ValueError(
            "compute_hours_required exceeds the amount of forecast time "
            "available between now and the deadline"
        )

    block_scores = eligible_df["score"].rolling(window=slots_required).sum()

    if block_scores.dropna().empty:
        raise ValueError("Could not compute a valid contiguous block schedule.")

    best_end_idx = block_scores.idxmin()
    best_start_idx = best_end_idx - slots_required + 1

    selected_df = eligible_df.copy()
    selected_df["run_flag"] = 0
    selected_df.loc[best_start_idx:best_end_idx, "run_flag"] = 1

    return selected_df


def optimize_schedule(
    forecast_df: pd.DataFrame,
    compute_hours_required: int,
    objective: str,
    deadline: str | None = None,
    schedule_mode: str = "flexible",
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
    schedule_mode : str
        Either:
        - 'flexible' : choose the best individual eligible intervals
        - 'block'    : choose one continuous run window

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

    if schedule_mode not in ["flexible", "block"]:
        raise ValueError("schedule_mode must be either 'flexible' or 'block'")

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

    interval_minutes = _infer_interval_minutes(df)
    rows_per_hour = 60 / interval_minutes
    slots_required = int(round(compute_hours_required * rows_per_hour))

    if slots_required <= 0:
        raise ValueError("Computed slots_required must be positive.")

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

    eligible_df = _build_score_column(eligible_df, objective)

    if schedule_mode == "flexible":
        selected_df = _select_flexible_schedule(eligible_df, slots_required)
    else:
        selected_df = _select_block_schedule(eligible_df, slots_required)

    df = df.merge(
        selected_df[["timestamp", "score", "run_flag"]],
        on="timestamp",
        how="left",
    )

    df["run_flag"] = df["run_flag"].fillna(0).astype(int)

    return df.sort_values("timestamp").reset_index(drop=True)