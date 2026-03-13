"""
Schedule formatting logic for Util.

This module converts optimizer output into a cleaner schedule table
that is easier to display in notebooks, Streamlit, and exports.
"""

import pandas as pd


def format_schedule(schedule_df: pd.DataFrame) -> pd.DataFrame:
    """
    Format an optimizer output dataframe into a clean schedule table.

    Expected columns
    ----------------
    - timestamp
    - carbon_g_per_kwh
    - price_per_kwh
    - eligible_flag
    - run_flag

    Returns
    -------
    pd.DataFrame
        Cleaned schedule table with human-readable action labels.
    """
    required_columns = {
        "timestamp",
        "carbon_g_per_kwh",
        "price_per_kwh",
        "eligible_flag",
        "run_flag",
    }

    if not required_columns.issubset(schedule_df.columns):
        raise ValueError(
            f"schedule_df must contain columns: {required_columns}"
        )

    df = schedule_df.copy()
    df = df.sort_values("timestamp").reset_index(drop=True)

    def classify_action(row):
        if row["eligible_flag"] == 0:
            return "Unavailable"
        if row["run_flag"] == 1:
            return "Run"
        return "Wait"

    df["recommended_action"] = df.apply(classify_action, axis=1)

    formatted_df = df[
        [
            "timestamp",
            "eligible_flag",
            "run_flag",
            "recommended_action",
            "price_per_kwh",
            "carbon_g_per_kwh",
        ]
    ].copy()

    return formatted_df