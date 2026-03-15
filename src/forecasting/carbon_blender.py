from __future__ import annotations

import pandas as pd

from src.forecasting.historical_loader import build_time_of_day_profile


def blend_live_forecast_with_history(
    live_forecast_df: pd.DataFrame,
    historical_df: pd.DataFrame,
    live_weight: float = 0.7,
    history_weight: float = 0.3,
) -> pd.DataFrame:
    """
    Blend live forecast values with recent historical average values
    for the same time of day.
    """
    if abs((live_weight + history_weight) - 1.0) > 1e-9:
        raise ValueError("live_weight and history_weight must sum to 1.0")

    live_df = live_forecast_df.copy()
    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"])

    if getattr(live_df["timestamp"].dt, "tz", None) is not None:
        live_df["timestamp"] = live_df["timestamp"].dt.tz_localize(None)

    live_df["time_key"] = live_df["timestamp"].dt.strftime("%H:%M")

    profile_df = build_time_of_day_profile(historical_df)

    merged = live_df.merge(profile_df, on="time_key", how="left")

    merged["historical_avg_carbon_g_per_kwh"] = merged[
        "historical_avg_carbon_g_per_kwh"
    ].fillna(merged["carbon_g_per_kwh"])

    merged["raw_live_carbon_g_per_kwh"] = merged["carbon_g_per_kwh"]

    merged["carbon_g_per_kwh"] = (
        live_weight * merged["raw_live_carbon_g_per_kwh"]
        + history_weight * merged["historical_avg_carbon_g_per_kwh"]
    )

    merged = merged.drop(columns=["time_key"])
    merged = merged.sort_values("timestamp").reset_index(drop=True)

    return merged