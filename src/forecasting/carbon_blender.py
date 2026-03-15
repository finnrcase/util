from __future__ import annotations

import pandas as pd

from src.forecasting.historical_loader import build_time_of_day_profile


LOCAL_TIMEZONE = "America/Los_Angeles"


def _normalize_timestamp_series(
    series: pd.Series,
    assume_utc_if_naive: bool = False,
) -> pd.Series:
    """
    Normalize a timestamp series to local naive datetimes.

    Behavior:
    - If the incoming timestamps are timezone-aware, convert them to local time
      and then drop timezone info.
    - If the incoming timestamps are naive and assume_utc_if_naive=True, treat
      them as UTC, convert to local time, then drop timezone info.
    - If the incoming timestamps are naive and assume_utc_if_naive=False, leave
      them as naive local-style timestamps.
    """
    parsed = pd.to_datetime(series, errors="coerce")

    tz = getattr(parsed.dt, "tz", None)

    if tz is not None:
        return parsed.dt.tz_convert(LOCAL_TIMEZONE).dt.tz_localize(None)

    if assume_utc_if_naive:
        return (
            pd.to_datetime(series, errors="coerce", utc=True)
            .dt.tz_convert(LOCAL_TIMEZONE)
            .dt.tz_localize(None)
        )

    return parsed


def _normalize_deadline(deadline: str | pd.Timestamp) -> pd.Timestamp:
    """
    Normalize a deadline to a local naive timestamp.
    """
    deadline_ts = pd.to_datetime(deadline)

    if getattr(deadline_ts, "tzinfo", None) is not None:
        return deadline_ts.tz_convert(LOCAL_TIMEZONE).tz_localize(None)

    return deadline_ts


def extend_forecast_with_history(
    live_forecast_df: pd.DataFrame,
    historical_df: pd.DataFrame,
    deadline: str | pd.Timestamp,
) -> pd.DataFrame:
    """
    Extend the live forecast beyond its available horizon using historical
    time-of-day averages.

    Rules:
    - Keep all live forecast rows exactly as-is.
    - If deadline exceeds the live forecast max timestamp, create extra rows
      at the same interval length.
    - For each extra timestamp, assign carbon using the historical average
      for that HH:MM slot.
    """
    required_live_columns = {"timestamp", "carbon_g_per_kwh"}
    if not required_live_columns.issubset(live_forecast_df.columns):
        raise ValueError(
            f"live_forecast_df must contain columns: {required_live_columns}"
        )

    required_historical_columns = {"timestamp", "carbon_g_per_kwh"}
    if not required_historical_columns.issubset(historical_df.columns):
        raise ValueError(
            f"historical_df must contain columns: {required_historical_columns}"
        )

    live_df = live_forecast_df.copy()
    historical_local_df = historical_df.copy()

    # WattTime timestamps are typically timezone-aware UTC strings.
    # If they arrive naive for some reason, we still treat them as UTC here.
    live_df["timestamp"] = _normalize_timestamp_series(
        live_df["timestamp"],
        assume_utc_if_naive=True,
    )
    historical_local_df["timestamp"] = _normalize_timestamp_series(
        historical_local_df["timestamp"],
        assume_utc_if_naive=True,
    )

    live_df = live_df.sort_values("timestamp").reset_index(drop=True)
    historical_local_df = historical_local_df.sort_values("timestamp").reset_index(drop=True)

    if live_df["timestamp"].isna().any():
        raise ValueError("live_forecast_df contains invalid timestamps.")

    if historical_local_df["timestamp"].isna().any():
        raise ValueError("historical_df contains invalid timestamps.")

    if len(live_df) < 2:
        raise ValueError(
            "live_forecast_df must contain at least 2 rows to extend forecast."
        )

    deadline_ts = _normalize_deadline(deadline)
    forecast_max = live_df["timestamp"].max()

    # No extension needed
    if deadline_ts <= forecast_max:
        live_df["carbon_source"] = "live_forecast"
        if "historical_avg_carbon_g_per_kwh" not in live_df.columns:
            live_df["historical_avg_carbon_g_per_kwh"] = pd.NA
        return live_df

    interval_minutes = (
        live_df["timestamp"]
        .sort_values()
        .diff()
        .dropna()
        .dt.total_seconds()
        .median()
        / 60
    )

    if pd.isna(interval_minutes) or interval_minutes <= 0:
        raise ValueError("Could not infer a valid live forecast interval.")

    interval = pd.Timedelta(minutes=interval_minutes)

    extension_timestamps = []
    next_ts = forecast_max + interval

    while next_ts <= deadline_ts:
        extension_timestamps.append(next_ts)
        next_ts += interval

    if not extension_timestamps:
        live_df["carbon_source"] = "live_forecast"
        if "historical_avg_carbon_g_per_kwh" not in live_df.columns:
            live_df["historical_avg_carbon_g_per_kwh"] = pd.NA
        return live_df

    profile_df = build_time_of_day_profile(historical_local_df)

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