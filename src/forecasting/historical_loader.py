from __future__ import annotations

import pandas as pd


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
    df = normalize_historical_dataframe(historical_df)
    df["time_key"] = df["timestamp"].dt.strftime("%H:%M")

    profile_df = (
        df.groupby("time_key", as_index=False)["carbon_g_per_kwh"]
        .mean()
        .rename(columns={"carbon_g_per_kwh": "historical_avg_carbon_g_per_kwh"})
    )

    return profile_df