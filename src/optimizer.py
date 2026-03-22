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

from src.scheduling_window import (
    build_eligibility_mask,
    calculate_required_slots,
    ensure_window_feasibility,
    InfeasibleScheduleError,
    INFEASIBLE_WORKLOAD_MESSAGE,
)


BALANCED_MISSING_DATA_MESSAGE = (
    "Balanced optimization requires both carbon and price data for every eligible interval."
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


def _min_max_normalize(series: pd.Series) -> pd.Series:
    """
    Normalize a numeric series to the [0, 1] range.
    Lower values remain better. If the series has no variation, return zeros.
    """
    numeric = pd.to_numeric(series, errors="coerce")
    min_value = numeric.min()
    max_value = numeric.max()

    if pd.isna(min_value) or pd.isna(max_value):
        raise ValueError("Cannot normalize a series with missing numeric values.")

    if max_value == min_value:
        return pd.Series(0.0, index=series.index)

    return (numeric - min_value) / (max_value - min_value)


def _validate_balanced_inputs(df: pd.DataFrame) -> None:
    required_columns = ["carbon_g_per_kwh", "price_per_kwh"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(BALANCED_MISSING_DATA_MESSAGE)

    if df[required_columns].isna().any().any():
        raise ValueError(BALANCED_MISSING_DATA_MESSAGE)


def _build_score_column(
    df: pd.DataFrame,
    objective: str,
    carbon_weight: float = 0.5,
    price_weight: float = 0.5,
) -> pd.DataFrame:
    """
    Add the optimization score column based on the selected objective.
    Lower score is better.
    """
    df = df.copy()

    if objective == "carbon":
        df["score"] = df["carbon_g_per_kwh"]
    elif objective == "cost":
        df["score"] = df["price_per_kwh"]
    elif objective == "balanced":
        _validate_balanced_inputs(df)
        df["normalized_carbon_metric"] = _min_max_normalize(df["carbon_g_per_kwh"])
        df["normalized_price_metric"] = _min_max_normalize(df["price_per_kwh"])
        df["score"] = (
            carbon_weight * df["normalized_carbon_metric"]
            + price_weight * df["normalized_price_metric"]
        )
    else:
        raise ValueError("objective must be 'carbon', 'cost', or 'balanced'")

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
    compute_hours_required: int,
    interval_minutes: float,
    objective: str,
    carbon_weight: float = 0.5,
    price_weight: float = 0.5,
) -> pd.DataFrame:
    """
    Select one contiguous run window with the lowest total score.

    Deadline remains a hard constraint because block mode only evaluates
    contiguous eligible intervals that already passed the now/deadline filter.
    This avoids treating a partial final hour before the deadline as a full
    usable block.
    """
    if compute_hours_required <= 0:
        raise ValueError("compute_hours_required must be positive")

    eligible_df = eligible_df.sort_values("timestamp").reset_index(drop=True).copy()
    if objective == "balanced":
        _validate_balanced_inputs(eligible_df)

    expected_interval = pd.to_timedelta(interval_minutes, unit="m")
    slots_required = calculate_required_slots(compute_hours_required, interval_minutes)
    eligible_df["interval_group"] = (
        eligible_df["timestamp"].diff().fillna(expected_interval).ne(expected_interval).cumsum()
    )

    best_candidate: tuple[float, int, int, int] | None = None

    for interval_group, group_df in eligible_df.groupby("interval_group", sort=False):
        group_df = group_df.reset_index(drop=True).copy()
        if len(group_df) < slots_required:
            continue

        if objective == "balanced":
            rolling_cost = group_df["price_per_kwh"].rolling(window=slots_required).sum()
            rolling_carbon = group_df["carbon_g_per_kwh"].rolling(window=slots_required).sum()
            candidate_df = pd.DataFrame(
                {
                    "end_idx": group_df.index,
                    "cost_total": rolling_cost,
                    "carbon_total": rolling_carbon,
                }
            ).dropna().reset_index(drop=True)

            if candidate_df.empty:
                continue

            candidate_df["normalized_price_metric"] = _min_max_normalize(candidate_df["cost_total"])
            candidate_df["normalized_carbon_metric"] = _min_max_normalize(candidate_df["carbon_total"])
            candidate_df["score"] = (
                carbon_weight * candidate_df["normalized_carbon_metric"]
                + price_weight * candidate_df["normalized_price_metric"]
            )
        else:
            candidate_df = pd.DataFrame(
                {
                    "end_idx": group_df.index,
                    "score": group_df["score"].rolling(window=slots_required).sum(),
                }
            ).dropna().reset_index(drop=True)

        if candidate_df.empty:
            continue

        best_row = candidate_df.loc[candidate_df["score"].idxmin()]
        selected_end_pos = int(best_row["end_idx"])
        selected_start_pos = selected_end_pos - slots_required + 1
        candidate = (
            float(best_row["score"]),
            int(interval_group),
            selected_start_pos,
            selected_end_pos,
        )

        if best_candidate is None or candidate[0] < best_candidate[0]:
            best_candidate = candidate

    if best_candidate is None:
        raise InfeasibleScheduleError(INFEASIBLE_WORKLOAD_MESSAGE)

    _, selected_interval_group, selected_start_pos, selected_end_pos = best_candidate

    selected_df = eligible_df.copy()
    selected_df["run_flag"] = 0
    selected_mask = (
        (selected_df["interval_group"] == selected_interval_group)
        & (selected_df.groupby("interval_group").cumcount() >= selected_start_pos)
        & (selected_df.groupby("interval_group").cumcount() <= selected_end_pos)
    )
    selected_df.loc[selected_mask, "run_flag"] = 1

    return selected_df.drop(columns=["interval_group"])


def optimize_schedule(
    forecast_df: pd.DataFrame,
    compute_hours_required: int,
    objective: str,
    deadline: str | None = None,
    schedule_mode: str = "flexible",
    current_time_override: str | None = None,
    carbon_weight: float = 0.5,
    price_weight: float = 0.5,
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
        One of 'carbon', 'cost', or 'balanced'.
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
    if objective not in ["carbon", "cost", "balanced"]:
        raise ValueError("objective must be 'carbon', 'cost', or 'balanced'")

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
    slots_required = calculate_required_slots(compute_hours_required, interval_minutes)

    if slots_required <= 0:
        raise ValueError("Computed slots_required must be positive.")

    eligible_mask, _, _ = build_eligibility_mask(
        timestamps=df["timestamp"],
        deadline=deadline,
        current_time_override=current_time_override,
    )
    # Extended forecast rows can exist beyond the user deadline, but they are
    # never selectable because eligibility is always clipped to now -> deadline
    # before flexible or block selection is evaluated.
    df["eligible_flag"] = eligible_mask.astype(int)

    eligible_df = df[df["eligible_flag"] == 1].copy()

    eligible_df = _build_score_column(
        eligible_df,
        objective,
        carbon_weight=carbon_weight,
        price_weight=price_weight,
    )

    if schedule_mode == "flexible":
        ensure_window_feasibility(slots_required, len(eligible_df))
        selected_df = _select_flexible_schedule(eligible_df, slots_required)
    else:
        selected_df = _select_block_schedule(
            eligible_df=eligible_df,
            compute_hours_required=compute_hours_required,
            interval_minutes=interval_minutes,
            objective=objective,
            carbon_weight=carbon_weight,
            price_weight=price_weight,
        )

    df = df.merge(
        selected_df[["timestamp", "score", "run_flag"]],
        on="timestamp",
        how="left",
    )

    df["run_flag"] = df["run_flag"].fillna(0).astype(int)

    return df.sort_values("timestamp").reset_index(drop=True)
