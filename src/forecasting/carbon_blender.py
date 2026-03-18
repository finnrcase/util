from __future__ import annotations

import pandas as pd

from src.forecasting.historical_loader import build_time_of_day_profile


DEFAULT_TOTAL_HORIZON_DAYS = 7


def extend_forecast_with_history(
    live_forecast_df: pd.DataFrame,
    historical_df: pd.DataFrame,
    deadline: str | pd.Timestamp,
    total_horizon_days: int = DEFAULT_TOTAL_HORIZON_DAYS,
) -> pd.DataFrame:
    """
    Extend the live forecast beyond its available horizon using historical
    time-of-day averages.

    Rules:
    - Keep all live forecast rows exactly as-is.
    - If the live forecast does not reach the target horizon, create extra rows
      at the same interval length after the live forecast ends.
    - For each extra timestamp, assign carbon using the historical average
      for that HH:MM slot.
    """
    live_df = live_forecast_df.copy()
    live_df["timestamp"] = (
        pd.to_datetime(live_df["timestamp"], utc=True)
        .dt.tz_convert("America/Los_Angeles")
        .dt.tz_localize(None)
    )

    live_df = live_df.sort_values("timestamp").reset_index(drop=True)

    if len(live_df) < 2:
        raise ValueError("live_forecast_df must contain at least 2 rows to extend forecast.")

    deadline_ts = pd.to_datetime(deadline)
    if getattr(deadline_ts, "tzinfo", None) is not None:
        deadline_ts = deadline_ts.tz_convert("America/Los_Angeles").tz_localize(None)

    forecast_min = live_df["timestamp"].min()
    forecast_max = live_df["timestamp"].max()
    target_horizon_ts = forecast_min + pd.Timedelta(days=total_horizon_days)
    extension_end_ts = max(deadline_ts, target_horizon_ts)

    # No extension needed
    if extension_end_ts <= forecast_max:
        live_df["carbon_source"] = "live_forecast"
        return live_df

    interval_minutes = (
        live_df["timestamp"].sort_values().diff().dropna().dt.total_seconds().median() / 60
    )

    if interval_minutes <= 0:
        raise ValueError("Could not infer a valid live forecast interval.")

    interval = pd.Timedelta(minutes=interval_minutes)

    extension_timestamps = []
    next_ts = forecast_max + interval

    while next_ts <= extension_end_ts:
        extension_timestamps.append(next_ts)
        next_ts += interval

    if not extension_timestamps:
        live_df["carbon_source"] = "live_forecast"
        return live_df

    profile_df = build_time_of_day_profile(historical_df)

    extension_df = pd.DataFrame({"timestamp": extension_timestamps})
    extension_df["time_key"] = extension_df["timestamp"].dt.strftime("%H:%M")

    extension_df = extension_df.merge(
        profile_df,
        on="time_key",
        how="left",
    )

    if extension_df["historical_avg_carbon_g_per_kwh"].isna().any():
        fallback_value = live_df["carbon_g_per_kwh"].iloc[-1]
        extension_df["historical_avg_carbon_g_per_kwh"] = extension_df[
            "historical_avg_carbon_g_per_kwh"
        ].fillna(fallback_value)

    extension_df["carbon_g_per_kwh"] = extension_df["historical_avg_carbon_g_per_kwh"]
    extension_df["carbon_source"] = "historical_expectation"

    keep_extension_cols = [
        "timestamp",
        "carbon_g_per_kwh",
        "historical_avg_carbon_g_per_kwh",
        "carbon_source",
    ]
    extension_df = extension_df[keep_extension_cols].copy()

    live_df["carbon_source"] = "live_forecast"

    if "historical_avg_carbon_g_per_kwh" not in live_df.columns:
        live_df["historical_avg_carbon_g_per_kwh"] = pd.NA

    combined_df = pd.concat([live_df, extension_df], ignore_index=True)
    combined_df = combined_df.sort_values("timestamp").reset_index(drop=True)

    return combined_df
