from __future__ import annotations

import pandas as pd


DEFAULT_TOTAL_HORIZON_DAYS = 7

# Hybrid historical-pattern weights for extension beyond the live forecast horizon.
RECENT_HISTORY_WEIGHT = 0.45
SAME_WEEKDAY_WEIGHT = 0.35
LONG_RUN_BASELINE_WEIGHT = 0.20

HYBRID_COMPONENT_WEIGHTS = {
    "recent_history": RECENT_HISTORY_WEIGHT,
    "same_weekday": SAME_WEEKDAY_WEIGHT,
    "long_run_baseline": LONG_RUN_BASELINE_WEIGHT,
}

RECENT_HISTORY_WINDOW_DAYS = 3


def _normalize_timestamp_column(df: pd.DataFrame, column: str = "timestamp") -> pd.DataFrame:
    normalized = df.copy()
    normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    normalized = normalized.dropna(subset=[column]).copy()

    if getattr(normalized[column].dt, "tz", None) is not None:
        normalized[column] = normalized[column].dt.tz_convert("America/Los_Angeles").dt.tz_localize(None)

    return normalized.sort_values(column).reset_index(drop=True)


def _prepare_history(
    historical_df: pd.DataFrame,
    *,
    value_column: str,
) -> pd.DataFrame:
    if value_column not in historical_df.columns:
        raise ValueError(f"historical_df must contain '{value_column}'")

    df = _normalize_timestamp_column(historical_df)
    df[value_column] = pd.to_numeric(df[value_column], errors="coerce")
    df = df.dropna(subset=[value_column]).copy()

    if df.empty:
        raise ValueError("historical_df does not contain usable numeric history.")

    df["time_key"] = df["timestamp"].dt.strftime("%H:%M")
    df["weekday"] = df["timestamp"].dt.dayofweek
    return df


def _weighted_component_average(components: dict[str, float | None]) -> float | None:
    valid_components = {
        name: value
        for name, value in components.items()
        if value is not None and not pd.isna(value)
    }
    if not valid_components:
        return None

    valid_weight_total = sum(HYBRID_COMPONENT_WEIGHTS[name] for name in valid_components)
    if valid_weight_total <= 0:
        return None

    return sum(
        (HYBRID_COMPONENT_WEIGHTS[name] / valid_weight_total) * float(value)
        for name, value in valid_components.items()
    )


def _build_component_profiles(
    historical_df: pd.DataFrame,
    *,
    value_column: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = _prepare_history(historical_df, value_column=value_column)
    latest_history_ts = df["timestamp"].max()
    recent_cutoff = latest_history_ts - pd.Timedelta(days=RECENT_HISTORY_WINDOW_DAYS)

    recent_profile_df = (
        df.loc[df["timestamp"] >= recent_cutoff]
        .groupby("time_key", as_index=False)[value_column]
        .mean()
        .rename(columns={value_column: "recent_history_component"})
    )
    same_weekday_profile_df = (
        df.groupby(["time_key", "weekday"], as_index=False)[value_column]
        .mean()
        .rename(columns={value_column: "same_weekday_component"})
    )
    baseline_profile_df = (
        df.groupby("time_key", as_index=False)[value_column]
        .mean()
        .rename(columns={value_column: "long_run_baseline_component"})
    )

    return recent_profile_df, same_weekday_profile_df, baseline_profile_df


def _build_hybrid_projection_profile(
    historical_df: pd.DataFrame,
    *,
    value_column: str,
    profile_value_column: str,
) -> pd.DataFrame:
    """
    Build a hybrid weighted profile using recent history, same-weekday history,
    and a longer-run baseline so extension is more stable than simply replaying
    the latest pattern.
    """
    df = _prepare_history(historical_df, value_column=value_column)
    recent_profile_df, same_weekday_profile_df, baseline_profile_df = _build_component_profiles(
        historical_df,
        value_column=value_column,
    )

    recent_lookup = recent_profile_df.set_index("time_key")["recent_history_component"].to_dict()
    same_weekday_lookup = same_weekday_profile_df.set_index(["time_key", "weekday"])["same_weekday_component"].to_dict()
    baseline_lookup = baseline_profile_df.set_index("time_key")["long_run_baseline_component"].to_dict()

    rows: list[dict[str, float | str]] = []
    for (time_key, weekday), group_df in df.groupby(["time_key", "weekday"], sort=False):
        hybrid_value = _weighted_component_average(
            {
                "recent_history": recent_lookup.get(time_key),
                "same_weekday": same_weekday_lookup.get((time_key, weekday)),
                "long_run_baseline": baseline_lookup.get(time_key),
            }
        )

        rows.append(
            {
                "time_key": time_key,
                "weekday": weekday,
                profile_value_column: hybrid_value,
            }
        )

    profile_df = pd.DataFrame(rows)
    if profile_df.empty:
        raise ValueError("Unable to build a hybrid historical profile from the provided history.")

    return profile_df


def build_time_of_day_profile(
    historical_df: pd.DataFrame,
    *,
    value_column: str,
    profile_value_column: str,
) -> pd.DataFrame:
    """
    Backward-compatible profile builder used by the extension engine.

    The returned profile now uses a hybrid weighted estimate:
    45% recent history, 35% same-weekday history, and 20% long-run baseline.
    """
    return _build_hybrid_projection_profile(
        historical_df,
        value_column=value_column,
        profile_value_column=profile_value_column,
    )


def extend_series_with_history(
    live_forecast_df: pd.DataFrame,
    historical_df: pd.DataFrame,
    *,
    deadline: str | pd.Timestamp,
    value_column: str,
    source_column: str,
    live_source_value: str,
    historical_source_value: str,
    profile_value_column: str,
    total_horizon_days: int = DEFAULT_TOTAL_HORIZON_DAYS,
) -> pd.DataFrame:
    """
    Extend a live time series beyond its horizon using a hybrid weighted
    historical-pattern estimate built from recent, same-weekday, and baseline
    history. Live forecast rows are preserved exactly as-is.
    """
    if value_column not in live_forecast_df.columns:
        raise ValueError(f"live_forecast_df must contain '{value_column}'")

    live_df = _normalize_timestamp_column(live_forecast_df)
    live_df[value_column] = pd.to_numeric(live_df[value_column], errors="coerce")
    live_df = live_df.dropna(subset=[value_column]).copy()

    if len(live_df) < 2:
        raise ValueError("live_forecast_df must contain at least 2 usable rows to extend forecast.")

    deadline_ts = pd.to_datetime(deadline)
    if getattr(deadline_ts, "tzinfo", None) is not None:
        deadline_ts = deadline_ts.tz_convert("America/Los_Angeles").tz_localize(None)

    forecast_min = live_df["timestamp"].min()
    forecast_max = live_df["timestamp"].max()
    target_horizon_ts = forecast_min + pd.Timedelta(days=total_horizon_days)
    extension_end_ts = max(deadline_ts, target_horizon_ts)

    live_df[source_column] = live_source_value
    if profile_value_column not in live_df.columns:
        live_df[profile_value_column] = pd.NA

    if extension_end_ts <= forecast_max:
        return live_df

    interval_minutes = (
        live_df["timestamp"].sort_values().diff().dropna().dt.total_seconds().median() / 60
    )
    if interval_minutes <= 0:
        raise ValueError("Could not infer a valid live forecast interval.")

    interval = pd.Timedelta(minutes=interval_minutes)
    extension_timestamps: list[pd.Timestamp] = []
    next_ts = forecast_max + interval

    while next_ts <= extension_end_ts:
        extension_timestamps.append(next_ts)
        next_ts += interval

    if not extension_timestamps:
        return live_df

    profile_df = build_time_of_day_profile(
        historical_df,
        value_column=value_column,
        profile_value_column=profile_value_column,
    )
    recent_profile_df, same_weekday_profile_df, baseline_profile_df = _build_component_profiles(
        historical_df,
        value_column=value_column,
    )
    recent_lookup = recent_profile_df.set_index("time_key")["recent_history_component"].to_dict()
    same_weekday_lookup = same_weekday_profile_df.set_index(["time_key", "weekday"])["same_weekday_component"].to_dict()
    baseline_lookup = baseline_profile_df.set_index("time_key")["long_run_baseline_component"].to_dict()

    extension_df = pd.DataFrame({"timestamp": extension_timestamps})
    extension_df["time_key"] = extension_df["timestamp"].dt.strftime("%H:%M")
    extension_df["weekday"] = extension_df["timestamp"].dt.dayofweek
    extension_df = extension_df.merge(
        profile_df[["time_key", "weekday", profile_value_column]],
        on=["time_key", "weekday"],
        how="left",
    )
    extension_df["recent_history_component"] = extension_df["time_key"].map(recent_lookup)
    extension_df["same_weekday_component"] = extension_df.apply(
        lambda row: same_weekday_lookup.get((row["time_key"], row["weekday"])),
        axis=1,
    )
    extension_df["long_run_baseline_component"] = extension_df["time_key"].map(baseline_lookup)
    extension_df[profile_value_column] = extension_df.apply(
        lambda row: _weighted_component_average(
            {
                "recent_history": row["recent_history_component"],
                "same_weekday": row["same_weekday_component"],
                "long_run_baseline": row["long_run_baseline_component"],
            }
        ),
        axis=1,
    )

    if extension_df[profile_value_column].isna().any():
        # Fallback to live tail value when even the hybrid history is too sparse for a slot.
        extension_df[profile_value_column] = extension_df[profile_value_column].fillna(live_df[value_column].iloc[-1])

    extension_df[value_column] = extension_df[profile_value_column]
    extension_df[source_column] = historical_source_value

    keep_columns = [
        "timestamp",
        value_column,
        profile_value_column,
        source_column,
    ]

    live_output_df = live_df[keep_columns].copy()
    extension_output_df = extension_df[keep_columns].copy()
    live_output_df[profile_value_column] = pd.to_numeric(
        live_output_df[profile_value_column],
        errors="coerce",
    )
    extension_output_df[profile_value_column] = pd.to_numeric(
        extension_output_df[profile_value_column],
        errors="coerce",
    )

    combined_df = pd.concat(
        [live_output_df, extension_output_df],
        ignore_index=True,
    )
    return combined_df.sort_values("timestamp").reset_index(drop=True)
