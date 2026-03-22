from __future__ import annotations

import pandas as pd
from scipy.optimize import nnls


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
MIN_TRAINING_ROWS = 12
MIN_VALIDATION_ROWS = 4
VALIDATION_FRACTION = 0.2


def _default_component_weights() -> dict[str, float]:
    return dict(HYBRID_COMPONENT_WEIGHTS)


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


def _normalize_component_weights(raw_weights: dict[str, float]) -> dict[str, float] | None:
    non_negative_weights = {name: max(float(value), 0.0) for name, value in raw_weights.items()}
    total_weight = sum(non_negative_weights.values())
    if total_weight <= 0:
        return None
    return {name: value / total_weight for name, value in non_negative_weights.items()}


def _weighted_component_average(
    components: dict[str, float | None],
    component_weights: dict[str, float] | None = None,
) -> float | None:
    valid_components = {
        name: value
        for name, value in components.items()
        if value is not None and not pd.isna(value)
    }
    if not valid_components:
        return None

    weights = component_weights or _default_component_weights()
    valid_weight_total = sum(weights[name] for name in valid_components)
    if valid_weight_total <= 0:
        return None

    return sum(
        (weights[name] / valid_weight_total) * float(value)
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


def _build_component_lookups(
    historical_df: pd.DataFrame,
    *,
    value_column: str,
) -> tuple[dict[str, float], dict[tuple[str, int], float], dict[str, float]]:
    recent_profile_df, same_weekday_profile_df, baseline_profile_df = _build_component_profiles(
        historical_df,
        value_column=value_column,
    )
    recent_lookup = recent_profile_df.set_index("time_key")["recent_history_component"].to_dict()
    same_weekday_lookup = same_weekday_profile_df.set_index(["time_key", "weekday"])["same_weekday_component"].to_dict()
    baseline_lookup = baseline_profile_df.set_index("time_key")["long_run_baseline_component"].to_dict()
    return recent_lookup, same_weekday_lookup, baseline_lookup


def _build_learning_examples(
    historical_df: pd.DataFrame,
    *,
    value_column: str,
) -> pd.DataFrame:
    df = _prepare_history(historical_df, value_column=value_column)
    examples: list[dict[str, float | pd.Timestamp | int | str]] = []

    for idx in range(1, len(df)):
        current_row = df.iloc[idx]
        prior_df = df.iloc[:idx].copy()
        if prior_df.empty:
            continue

        recent_cutoff = current_row["timestamp"] - pd.Timedelta(days=RECENT_HISTORY_WINDOW_DAYS)
        recent_component = prior_df.loc[
            (prior_df["timestamp"] >= recent_cutoff) & (prior_df["time_key"] == current_row["time_key"]),
            value_column,
        ].mean()
        same_weekday_component = prior_df.loc[
            (prior_df["time_key"] == current_row["time_key"]) & (prior_df["weekday"] == current_row["weekday"]),
            value_column,
        ].mean()
        baseline_component = prior_df.loc[
            prior_df["time_key"] == current_row["time_key"],
            value_column,
        ].mean()

        examples.append(
            {
                "timestamp": current_row["timestamp"],
                "time_key": current_row["time_key"],
                "weekday": current_row["weekday"],
                "recent_history_component": recent_component,
                "same_weekday_component": same_weekday_component,
                "long_run_baseline_component": baseline_component,
                "target_value": current_row[value_column],
            }
        )

    examples_df = pd.DataFrame(examples)
    if examples_df.empty:
        return examples_df

    return examples_df.sort_values("timestamp").reset_index(drop=True)


def _score_component_weights(
    examples_df: pd.DataFrame,
    component_weights: dict[str, float],
) -> float:
    predictions = examples_df.apply(
        lambda row: _weighted_component_average(
            {
                "recent_history": row["recent_history_component"],
                "same_weekday": row["same_weekday_component"],
                "long_run_baseline": row["long_run_baseline_component"],
            },
            component_weights=component_weights,
        ),
        axis=1,
    )
    valid_predictions = pd.to_numeric(predictions, errors="coerce")
    valid_mask = valid_predictions.notna() & pd.to_numeric(examples_df["target_value"], errors="coerce").notna()
    if not valid_mask.any():
        return float("inf")

    errors = (
        pd.to_numeric(examples_df.loc[valid_mask, "target_value"], errors="coerce")
        - valid_predictions.loc[valid_mask]
    ).abs()
    return float(errors.mean())


def _learn_component_weights(
    historical_df: pd.DataFrame,
    *,
    value_column: str,
) -> dict[str, object]:
    """
    Learn non-negative blend weights from past-only historical examples using
    a time-ordered train/validation split. Falls back to fixed weights when
    history is too sparse to fit a stable model.
    """
    default_weights = _default_component_weights()
    examples_df = _build_learning_examples(
        historical_df,
        value_column=value_column,
    )

    complete_examples_df = examples_df.dropna(
        subset=[
            "recent_history_component",
            "same_weekday_component",
            "long_run_baseline_component",
            "target_value",
        ]
    ).copy()

    validation_rows = max(int(len(complete_examples_df) * VALIDATION_FRACTION), MIN_VALIDATION_ROWS)
    if len(complete_examples_df) < (MIN_TRAINING_ROWS + MIN_VALIDATION_ROWS) or len(complete_examples_df) <= validation_rows:
        return {
            "weights": default_weights,
            "method": "fixed_fallback_insufficient_history",
            "training_rows": int(len(complete_examples_df)),
            "validation_rows": 0,
            "validation_mae": None,
            "baseline_validation_mae": None,
        }

    train_df = complete_examples_df.iloc[:-validation_rows].copy()
    validation_df = complete_examples_df.iloc[-validation_rows:].copy()

    if len(train_df) < MIN_TRAINING_ROWS or validation_df.empty:
        return {
            "weights": default_weights,
            "method": "fixed_fallback_insufficient_history",
            "training_rows": int(len(train_df)),
            "validation_rows": int(len(validation_df)),
            "validation_mae": None,
            "baseline_validation_mae": None,
        }

    feature_columns = [
        "recent_history_component",
        "same_weekday_component",
        "long_run_baseline_component",
    ]
    x_train = train_df[feature_columns].to_numpy(dtype=float)
    y_train = train_df["target_value"].to_numpy(dtype=float)

    learned_raw_weights, _ = nnls(x_train, y_train)
    normalized_weights = _normalize_component_weights(
        {
            "recent_history": learned_raw_weights[0],
            "same_weekday": learned_raw_weights[1],
            "long_run_baseline": learned_raw_weights[2],
        }
    )

    if normalized_weights is None:
        return {
            "weights": default_weights,
            "method": "fixed_fallback_zero_weight_fit",
            "training_rows": int(len(train_df)),
            "validation_rows": int(len(validation_df)),
            "validation_mae": None,
            "baseline_validation_mae": None,
        }

    validation_mae = _score_component_weights(validation_df, normalized_weights)
    baseline_validation_mae = _score_component_weights(validation_df, default_weights)

    return {
        "weights": normalized_weights,
        "method": "learned_nnls",
        "training_rows": int(len(train_df)),
        "validation_rows": int(len(validation_df)),
        "validation_mae": validation_mae,
        "baseline_validation_mae": baseline_validation_mae,
    }


def _build_hybrid_projection_profile(
    historical_df: pd.DataFrame,
    *,
    value_column: str,
    profile_value_column: str,
    component_weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    """
    Build a hybrid weighted profile using recent history, same-weekday history,
    and a longer-run baseline so extension is more stable than simply replaying
    the latest pattern.
    """
    df = _prepare_history(historical_df, value_column=value_column)
    active_weights = component_weights or _default_component_weights()
    recent_lookup, same_weekday_lookup, baseline_lookup = _build_component_lookups(
        historical_df,
        value_column=value_column,
    )

    rows: list[dict[str, float | str]] = []
    for (time_key, weekday), group_df in df.groupby(["time_key", "weekday"], sort=False):
        hybrid_value = _weighted_component_average(
            {
                "recent_history": recent_lookup.get(time_key),
                "same_weekday": same_weekday_lookup.get((time_key, weekday)),
                "long_run_baseline": baseline_lookup.get(time_key),
            },
            component_weights=active_weights,
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

    The returned profile uses a hybrid estimate whose blend weights are learned
    from historical data when possible, with a fixed weighted fallback when
    history is too sparse.
    """
    learning_summary = _learn_component_weights(
        historical_df,
        value_column=value_column,
    )
    profile_df = _build_hybrid_projection_profile(
        historical_df,
        value_column=value_column,
        profile_value_column=profile_value_column,
        component_weights=learning_summary["weights"],
    )
    profile_df.attrs["extension_model"] = learning_summary
    print(
        f"[FORECAST EXTENSION DEBUG] {value_column} weights={learning_summary['weights']} "
        f"method={learning_summary['method']} "
        f"training_rows={learning_summary['training_rows']} "
        f"validation_rows={learning_summary['validation_rows']} "
        f"validation_mae={learning_summary['validation_mae']} "
        f"baseline_validation_mae={learning_summary['baseline_validation_mae']}"
    )
    return profile_df


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
    learning_summary = profile_df.attrs.get(
        "extension_model",
        {
            "weights": _default_component_weights(),
            "method": "fixed_fallback_unknown",
            "training_rows": 0,
            "validation_rows": 0,
            "validation_mae": None,
            "baseline_validation_mae": None,
        },
    )
    component_weights = learning_summary["weights"]
    recent_lookup, same_weekday_lookup, baseline_lookup = _build_component_lookups(
        historical_df,
        value_column=value_column,
    )

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
            },
            component_weights=component_weights,
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
    combined_df = combined_df.sort_values("timestamp").reset_index(drop=True)
    combined_df.attrs["extension_model"] = learning_summary
    return combined_df
