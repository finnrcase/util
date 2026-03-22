from __future__ import annotations

import pandas as pd

from src.forecasting.pattern_extension import build_time_of_day_profile as build_generic_time_of_day_profile


def normalize_historical_dataframe(historical_df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize historical carbon dataframe timestamps and sort rows.
    """
    df = historical_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    if getattr(df["timestamp"].dt, "tz", None) is not None:
        df["timestamp"] = df["timestamp"].dt.tz_localize(None)

    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def build_time_of_day_profile(historical_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build an average historical carbon profile by HH:MM time-of-day.
    """
    return build_generic_time_of_day_profile(
        historical_df,
        value_column="carbon_g_per_kwh",
        profile_value_column="historical_avg_carbon_g_per_kwh",
    )
