"""
Metrics calculations for Util.

This module compares optimized and baseline schedules.
"""

import pandas as pd


def _infer_interval_minutes(df: pd.DataFrame) -> float:
    """
    Infer the time interval in minutes between forecast rows.
    Assumes timestamp column is sorted.
    """
    if len(df) < 2:
        raise ValueError("schedule_df must contain at least 2 rows to infer interval.")

    ts = pd.to_datetime(df["timestamp"]).sort_values()
    diffs = ts.diff().dropna()
    interval_minutes = diffs.dt.total_seconds().median() / 60

    if interval_minutes <= 0:
        raise ValueError("Could not infer a valid interval length.")

    return interval_minutes


def calculate_schedule_totals(
    schedule_df: pd.DataFrame,
    run_flag_column: str,
    machine_watts: int,
) -> dict:
    """
    Calculate total cost and total carbon for a given schedule.

    Parameters
    ----------
    schedule_df : pd.DataFrame
        Must contain:
        - timestamp
        - carbon_g_per_kwh
        - price_per_kwh
        - run flag column
    run_flag_column : str
        Column indicating whether the workload runs in each interval.
    machine_watts : int
        Machine power draw in watts.

    Returns
    -------
    dict
        Dictionary with total_cost and total_carbon_kg.
    """
    required_columns = {"timestamp", "carbon_g_per_kwh", "price_per_kwh", run_flag_column}
    if not required_columns.issubset(schedule_df.columns):
        raise ValueError(f"schedule_df must contain columns: {required_columns}")

    df = schedule_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    interval_minutes = _infer_interval_minutes(df)
    interval_hours = interval_minutes / 60.0
    machine_kw = machine_watts / 1000.0

    df["interval_cost"] = (
        df[run_flag_column] * machine_kw * interval_hours * df["price_per_kwh"]
    )
    df["interval_carbon_kg"] = (
        df[run_flag_column] * machine_kw * interval_hours * df["carbon_g_per_kwh"] / 1000.0
    )

    total_cost = df["interval_cost"].sum()
    total_carbon_kg = df["interval_carbon_kg"].sum()

    return {
        "total_cost": total_cost,
        "total_carbon_kg": total_carbon_kg,
    }


def compare_schedules(
    baseline_df: pd.DataFrame,
    optimized_df: pd.DataFrame,
    machine_watts: int,
) -> dict:
    """
    Compare baseline and optimized schedules.

    Returns
    -------
    dict
        Summary metrics for cost and carbon performance.
    """
    baseline_totals = calculate_schedule_totals(
        schedule_df=baseline_df,
        run_flag_column="baseline_run_flag",
        machine_watts=machine_watts,
    )

    optimized_totals = calculate_schedule_totals(
        schedule_df=optimized_df,
        run_flag_column="run_flag",
        machine_watts=machine_watts,
    )

    baseline_cost = baseline_totals["total_cost"]
    optimized_cost = optimized_totals["total_cost"]

    baseline_carbon = baseline_totals["total_carbon_kg"]
    optimized_carbon = optimized_totals["total_carbon_kg"]

    cost_savings = baseline_cost - optimized_cost
    carbon_savings_kg = baseline_carbon - optimized_carbon

    cost_reduction_pct = (
        (cost_savings / baseline_cost) * 100 if baseline_cost > 0 else 0
    )

    carbon_reduction_pct = (
        (carbon_savings_kg / baseline_carbon) * 100 if baseline_carbon > 0 else 0
    )

    return {
        "baseline_cost": baseline_cost,
        "optimized_cost": optimized_cost,
        "cost_savings": cost_savings,
        "cost_reduction_pct": cost_reduction_pct,
        "baseline_carbon_kg": baseline_carbon,
        "optimized_carbon_kg": optimized_carbon,
        "carbon_savings_kg": carbon_savings_kg,
        "carbon_reduction_pct": carbon_reduction_pct,
    }