"""
Data loading utilities for Util.
"""

from pathlib import Path
import logging
import time

import pandas as pd

from services.watttime_service import get_watttime_forecast, get_watttime_historical
from src.forecasting.carbon_blender import extend_forecast_with_history
from src.forecasting.pattern_extension import extend_series_with_history
from src.pricing import PricingUnavailableError, align_price_series, get_price_series


data_fetcher_logger = logging.getLogger("uvicorn.error")


def _normalize_timestamp_column(df: pd.DataFrame, column: str = "timestamp") -> pd.DataFrame:
    df = df.copy()
    df[column] = (
        pd.to_datetime(df[column], utc=True)
        .dt.tz_convert("America/Los_Angeles")
        .dt.tz_localize(None)
    )
    return df


def _coerce_local_timestamp(value: str | None) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None

    if getattr(ts, "tzinfo", None) is None:
        return ts.tz_localize("America/Los_Angeles")
    return ts.tz_convert("America/Los_Angeles")


def _deadline_exceeds_live_horizon(deadline: str | None, live_df: pd.DataFrame) -> bool:
    deadline_ts = _coerce_local_timestamp(deadline)
    if deadline_ts is None or live_df.empty:
        return False

    live_max = pd.to_datetime(live_df["timestamp"], errors="coerce").dropna().max()
    if pd.isna(live_max):
        return False

    if getattr(live_max, "tzinfo", None) is None:
        live_max = live_max.tz_localize("America/Los_Angeles")
    else:
        live_max = live_max.tz_convert("America/Los_Angeles")

    return deadline_ts > live_max


def load_carbon_forecast(filepath: str | Path) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    required_columns = {"timestamp", "carbon_g_per_kwh"}

    if not required_columns.issubset(df.columns):
        raise ValueError(
            f"Carbon forecast file must contain columns: {required_columns}"
        )

    df = _normalize_timestamp_column(df, "timestamp")
    return df


def load_price_forecast(filepath: str | Path) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    required_columns = {"timestamp", "price_per_kwh"}

    if not required_columns.issubset(df.columns):
        raise ValueError(
            f"Price forecast file must contain columns: {required_columns}"
        )

    df = _normalize_timestamp_column(df, "timestamp")
    return df


def build_forecast_table(
    carbon_filepath: str | Path,
    price_filepath: str | Path,
) -> pd.DataFrame:
    carbon_df = load_carbon_forecast(carbon_filepath)
    price_df = load_price_forecast(price_filepath)
    carbon_df["carbon_source"] = "live_forecast"
    price_df["price_signal_source"] = "live_forecast"

    forecast_df = pd.merge(carbon_df, price_df, on="timestamp", how="inner")
    forecast_df = forecast_df.sort_values("timestamp").reset_index(drop=True)

    if forecast_df.empty:
        raise ValueError("Merged forecast table is empty. Check timestamp alignment.")

    return forecast_df


def _fetch_live_forecast_for_region(region: str):
    started_at = time.perf_counter()
    data_fetcher_logger.info("Util forecast: WattTime carbon fetch start region=%s", region)
    carbon_df = get_watttime_forecast(region)
    region_used = region
    access_mode = "direct_region"
    data_fetcher_logger.info(
        "Util forecast: WattTime carbon fetch success requested_region=%s region_used=%s access_mode=%s rows=%s elapsed_ms=%.1f",
        region,
        region_used,
        access_mode,
        len(carbon_df),
        (time.perf_counter() - started_at) * 1000.0,
    )
    return carbon_df, region_used, access_mode


def _fetch_live_historical_for_region(region: str, days: int):
    started_at = time.perf_counter()
    data_fetcher_logger.info("Util forecast: WattTime historical fetch start region=%s days=%s", region, days)
    historical_df = get_watttime_historical(region=region, days=days)
    region_used = region
    data_fetcher_logger.info(
        "Util forecast: WattTime historical fetch success requested_region=%s region_used=%s rows=%s elapsed_ms=%.1f",
        region,
        region_used,
        len(historical_df),
        (time.perf_counter() - started_at) * 1000.0,
    )
    return historical_df, region_used


def _infer_interval_minutes_from_timestamps(timestamps: pd.Series) -> float:
    normalized = pd.to_datetime(timestamps, errors="coerce").dropna().sort_values()
    if len(normalized) < 2:
        raise ValueError("At least 2 timestamps are required to infer interval length.")

    interval_minutes = normalized.diff().dropna().dt.total_seconds().median() / 60.0
    if interval_minutes <= 0:
        raise ValueError("Could not infer a valid interval length.")

    return float(interval_minutes)


def _build_historical_price_template(
    historical_price_df: pd.DataFrame,
    *,
    interval_minutes: float,
) -> pd.DataFrame:
    history_df = historical_price_df.copy()
    history_df["timestamp"] = pd.to_datetime(history_df["timestamp"], errors="coerce")
    history_df = history_df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    if history_df.empty:
        raise PricingUnavailableError("Historical price fetch returned no usable rows.")

    template_timestamps = pd.date_range(
        start=history_df["timestamp"].min(),
        end=history_df["timestamp"].max(),
        freq=pd.Timedelta(minutes=interval_minutes),
    )
    template_df = pd.DataFrame({"timestamp": template_timestamps}).sort_values("timestamp")
    aligned = pd.merge_asof(
        template_df,
        history_df.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )
    aligned["price_per_kwh"] = aligned["price_per_kwh"].ffill().bfill()
    return aligned


def build_live_historical_export_table(
    region: str,
    days: int = 14,
) -> pd.DataFrame:
    historical_df, historical_region_used = _fetch_live_historical_for_region(
        region,
        days,
    )
    historical_df = _normalize_timestamp_column(historical_df, "timestamp")
    historical_df["historical_region_used"] = historical_region_used
    return historical_df.sort_values("timestamp").reset_index(drop=True)


def build_live_price_forecast_table(
    *,
    region: str,
    target_timestamps: pd.Series,
    live_target_timestamps: pd.Series | None = None,
    historical_days: int,
    deadline: str | None,
    allow_historical_extension: bool,
) -> pd.DataFrame:
    target_ts = pd.to_datetime(target_timestamps, errors="coerce").dropna().sort_values()
    if target_ts.empty:
        raise PricingUnavailableError("No target timestamps were provided for price forecasting.")

    live_target_ts = pd.to_datetime(
        live_target_timestamps if live_target_timestamps is not None else target_timestamps,
        errors="coerce",
    ).dropna().sort_values()
    if live_target_ts.empty:
        live_target_ts = target_ts

    interval_minutes = _infer_interval_minutes_from_timestamps(target_ts)
    price_started_at = time.perf_counter()
    data_fetcher_logger.info("Util forecast: live price fetch start region=%s", region)
    live_price_df = get_price_series(
        region_code=region,
        start_time=live_target_ts.min(),
        end_time=live_target_ts.max(),
    )
    data_fetcher_logger.info(
        "Util forecast: live price fetch success region=%s rows=%s elapsed_ms=%.1f",
        region,
        len(live_price_df),
        (time.perf_counter() - price_started_at) * 1000.0,
    )

    live_aligned_df = align_price_series(
        price_df=live_price_df,
        target_timestamps=live_target_ts,
        carry_forward_beyond_last_known=False,
    )
    if "price_node" in live_price_df.columns:
        live_aligned_df["price_node"] = live_price_df["price_node"].dropna().iloc[0]
    live_aligned_df["price_signal_source"] = "live_forecast"

    live_price_max = pd.to_datetime(live_price_df["timestamp"], errors="coerce").dropna().max()
    if pd.isna(live_price_max):
        raise PricingUnavailableError("Live price fetch returned no usable timestamps.")

    needs_extension = bool((target_ts > live_price_max).any())
    if not needs_extension:
        if "price_node" in live_price_df.columns and "price_node" not in live_aligned_df.columns:
            live_aligned_df["price_node"] = live_price_df["price_node"].dropna().iloc[0]
        live_aligned_df["price_extension_status"] = "not_needed"
        live_aligned_df["price_extension_message"] = ""
        return live_aligned_df

    if not allow_historical_extension:
        live_aligned_df["price_extension_status"] = "live_only"
        live_aligned_df["price_extension_message"] = (
            "Live price data does not cover the requested optimization horizon, so Util kept live rows only."
        )
        data_fetcher_logger.info("Util forecast: historical price fetch skipped region=%s reason=extension_not_enabled", region)
        return live_aligned_df

    if deadline is None:
        live_aligned_df["price_extension_status"] = "live_only"
        live_aligned_df["price_extension_message"] = (
            "Price historical-pattern extension requires a deadline, so Util kept live rows only."
        )
        data_fetcher_logger.info("Util forecast: historical price fetch skipped region=%s reason=missing_deadline", region)
        return live_aligned_df

    try:
        history_end = live_price_max - pd.Timedelta(minutes=interval_minutes)
        history_start = history_end - pd.Timedelta(days=historical_days)
        history_started_at = time.perf_counter()
        data_fetcher_logger.info("Util forecast: historical price fetch start region=%s days=%s", region, historical_days)
        historical_price_df = get_price_series(
            region_code=region,
            start_time=history_start,
            end_time=history_end,
        )
        data_fetcher_logger.info(
            "Util forecast: historical price fetch success region=%s rows=%s elapsed_ms=%.1f",
            region,
            len(historical_price_df),
            (time.perf_counter() - history_started_at) * 1000.0,
        )

        historical_template_df = _build_historical_price_template(
            historical_price_df,
            interval_minutes=interval_minutes,
        )

        live_extension_seed_df = live_aligned_df.dropna(subset=["price_per_kwh"]).copy()
        if len(live_extension_seed_df) < 2:
            raise PricingUnavailableError("Not enough live price rows were available to build an estimated extension.")

        extended_price_df = extend_series_with_history(
            live_forecast_df=live_extension_seed_df[["timestamp", "price_per_kwh"]].copy(),
            historical_df=historical_template_df[["timestamp", "price_per_kwh"]].copy(),
            deadline=deadline,
            value_column="price_per_kwh",
            source_column="price_signal_source",
            live_source_value="live_forecast",
            historical_source_value="historical_pattern_estimate",
            profile_value_column="historical_avg_price_per_kwh",
        )

        combined_df = pd.merge(
            pd.DataFrame({"timestamp": target_ts}),
            extended_price_df,
            on="timestamp",
            how="left",
        )

        metadata_columns = [column for column in ["source", "region_code", "price_node"] if column in live_aligned_df.columns]
        if metadata_columns:
            metadata_df = live_aligned_df[["timestamp"] + metadata_columns].drop_duplicates(subset=["timestamp"])
            combined_df = combined_df.merge(metadata_df, on="timestamp", how="left")
            for column in metadata_columns:
                combined_df[column] = combined_df[column].ffill().bfill()

        combined_df["price_extension_status"] = "extended"
        combined_df["price_extension_message"] = ""
        return combined_df
    except Exception as exc:
        data_fetcher_logger.warning("Util forecast: historical price fetch fallback region=%s detail=%s", region, exc)
        live_aligned_df["price_extension_status"] = "live_only_extension_failed"
        live_aligned_df["price_extension_message"] = (
            f"Historical-pattern price extension failed; Util kept live price rows only. Details: {exc}"
        )
        return live_aligned_df


def build_live_carbon_forecast_table(
    region: str,
    placeholder_price_per_kwh: float = 0.15,
    carbon_estimation_mode: str = "forecast_only",
    historical_days: int = 7,
    deadline: str | None = None,
) -> pd.DataFrame:
    requested_region = region

    total_started_at = time.perf_counter()
    data_fetcher_logger.info("Util forecast: live carbon table start requested_region=%s carbon_mode=%s", requested_region, carbon_estimation_mode)
    carbon_df, forecast_region_used, forecast_access_mode = _fetch_live_forecast_for_region(
        requested_region
    )

    required_columns = {"timestamp", "carbon_g_per_kwh"}
    if not required_columns.issubset(carbon_df.columns):
        raise ValueError(
            f"WattTime forecast must contain columns: {required_columns}"
        )

    carbon_df = _normalize_timestamp_column(carbon_df, "timestamp")

    if carbon_estimation_mode == "forecast_plus_historical_expectation":
        needs_historical_carbon = _deadline_exceeds_live_horizon(deadline, carbon_df)
        if deadline is None:
            raise ValueError(
                "forecast_plus_historical_expectation mode requires deadline."
            )

        if needs_historical_carbon:
            historical_df, historical_region_used = _fetch_live_historical_for_region(
                requested_region,
                historical_days,
            )
            historical_df = _normalize_timestamp_column(historical_df, "timestamp")
            carbon_df = extend_forecast_with_history(
                live_forecast_df=carbon_df,
                historical_df=historical_df,
                deadline=deadline,
            )
            carbon_df["historical_region_used"] = historical_region_used
        else:
            carbon_df["carbon_source"] = "live_forecast"
            data_fetcher_logger.info(
                "Util forecast: carbon historical fetch skipped region=%s reason=live_forecast_covers_deadline",
                requested_region,
            )

    elif carbon_estimation_mode == "forecast_only":
        carbon_df["carbon_source"] = "live_forecast"

    else:
        raise ValueError(
            "carbon_estimation_mode must be either "
            "'forecast_only' or 'forecast_plus_historical_expectation'"
        )

    pricing_status = "placeholder"
    pricing_message = (
        "Live electricity pricing is not available for this resolved region yet. "
        "Util is using clearly labeled fallback pricing."
    )
    pricing_source = "Fallback Pricing"
    pricing_region_code = requested_region
    price_signal_source = "placeholder"

    try:
        live_price_target_ts = (
            carbon_df.loc[carbon_df["carbon_source"] == "live_forecast", "timestamp"]
            if "carbon_source" in carbon_df.columns
            else carbon_df["timestamp"]
        )
        if live_price_target_ts.empty:
            live_price_target_ts = carbon_df["timestamp"]

        pricing_df = build_live_price_forecast_table(
            region=forecast_region_used,
            target_timestamps=carbon_df["timestamp"],
            live_target_timestamps=live_price_target_ts,
            historical_days=historical_days,
            deadline=deadline,
            allow_historical_extension=(carbon_estimation_mode == "forecast_plus_historical_expectation"),
        )
        forecast_df = pd.merge(
            carbon_df,
            pricing_df,
            on="timestamp",
            how="left",
        )
        forecast_df["price_per_kwh"] = pd.to_numeric(
            forecast_df["price_per_kwh"],
            errors="coerce",
        )
        if forecast_df["price_per_kwh"].isna().all():
            raise PricingUnavailableError(
                f"No live price rows aligned to WattTime region '{forecast_region_used}'."
            )
        missing_price_rows = int(forecast_df["price_per_kwh"].isna().sum())
        if missing_price_rows > 0:
            forecast_df["price_per_kwh"] = forecast_df["price_per_kwh"].fillna(placeholder_price_per_kwh)
            missing_mask = forecast_df["price_signal_source"].isna()
            if missing_mask.any():
                forecast_df.loc[missing_mask, "price_signal_source"] = "placeholder"
            data_fetcher_logger.warning(
                "Util forecast: filled uncovered live price rows with fallback pricing count=%s region=%s",
                missing_price_rows,
                forecast_region_used,
            )
        pricing_status = "live_market"
        extension_status = (
            forecast_df["price_extension_status"].dropna().iloc[0]
            if "price_extension_status" in forecast_df.columns and forecast_df["price_extension_status"].dropna().any()
            else "not_needed"
        )
        extension_message = (
            forecast_df["price_extension_message"].dropna().iloc[0]
            if "price_extension_message" in forecast_df.columns and forecast_df["price_extension_message"].dropna().any()
            else ""
        )
        provider_label = (
            forecast_df["source_provider"].dropna().iloc[0]
            if "source_provider" in forecast_df.columns and forecast_df["source_provider"].dropna().any()
            else "live market"
        )
        market_label = (
            forecast_df["source_market"].dropna().iloc[0]
            if "source_market" in forecast_df.columns and forecast_df["source_market"].dropna().any()
            else "day-ahead"
        )
        pricing_message = (
            f"Using {provider_label} {market_label.lower()} pricing where live rows are available, "
            "with historical-pattern extension beyond the live horizon when needed."
            if (forecast_df.get("price_signal_source") == "historical_pattern_estimate").any()
            else f"Using {provider_label} {market_label.lower()} pricing routed from the resolved WattTime region."
        )
        if extension_status in {"live_only", "live_only_extension_failed"} and extension_message:
            pricing_message = (
                f"Using live {provider_label} pricing where available. "
                "Rows beyond the live horizon fell back to fallback pricing. "
                f"{extension_message}"
            )
        pricing_source = (
            forecast_df["source_provider"].dropna().iloc[0]
            if "source_provider" in forecast_df.columns and forecast_df["source_provider"].dropna().any()
            else (
                forecast_df["source"].dropna().iloc[0]
                if "source" in forecast_df.columns and forecast_df["source"].dropna().any()
                else "Unknown Live Price Source"
            )
        )
        pricing_region_code = (
            forecast_df["region_code"].dropna().iloc[0]
            if "region_code" in forecast_df.columns and forecast_df["region_code"].dropna().any()
            else forecast_region_used
        )
        pricing_node = (
            forecast_df["node_or_zone"].dropna().iloc[0]
            if "node_or_zone" in forecast_df.columns and forecast_df["node_or_zone"].dropna().any()
            else (
                forecast_df["price_node"].dropna().iloc[0]
                if "price_node" in forecast_df.columns and forecast_df["price_node"].dropna().any()
                else ""
            )
        )
        pricing_market = (
            forecast_df["source_market"].dropna().iloc[0]
            if "source_market" in forecast_df.columns and forecast_df["source_market"].dropna().any()
            else ""
        )
        price_signal_source = (
            forecast_df["price_signal_source"].dropna().iloc[0]
            if "price_signal_source" in forecast_df.columns and forecast_df["price_signal_source"].dropna().any()
            else "live_forecast"
        )
    except PricingUnavailableError as exc:
        data_fetcher_logger.warning("Util forecast: pricing fetch fallback region=%s detail=%s", forecast_region_used, exc)
        forecast_df = carbon_df.copy()
        forecast_df["price_per_kwh"] = placeholder_price_per_kwh
        forecast_df["price_signal_source"] = "placeholder"
        pricing_message = str(exc)
        pricing_node = ""
        if "historical_avg_price_per_kwh" not in forecast_df.columns:
            forecast_df["historical_avg_price_per_kwh"] = pd.NA
        forecast_df["price_extension_status"] = "placeholder"
        forecast_df["price_extension_message"] = pricing_message

    data_fetcher_logger.info(
        "Util forecast: live carbon table ready region=%s rows=%s non_null_price_rows=%s elapsed_ms=%.1f",
        forecast_region_used,
        len(forecast_df),
        int(pd.to_numeric(forecast_df["price_per_kwh"], errors="coerce").notna().sum()),
        (time.perf_counter() - total_started_at) * 1000.0,
    )

    forecast_df["forecast_region_requested"] = requested_region
    forecast_df["forecast_region_used"] = forecast_region_used
    forecast_df["forecast_access_mode"] = forecast_access_mode
    forecast_df["pricing_status"] = pricing_status
    forecast_df["pricing_message"] = pricing_message
    forecast_df["pricing_source"] = pricing_source
    forecast_df["pricing_market"] = pricing_market if "pricing_market" in locals() else ""
    forecast_df["pricing_region_code"] = pricing_region_code
    forecast_df["pricing_node"] = pricing_node
    forecast_df["price_signal_source"] = forecast_df.get("price_signal_source", price_signal_source)

    ordered_columns = [
        col for col in [
            "timestamp",
            "carbon_g_per_kwh",
            "price_per_kwh",
            "historical_avg_carbon_g_per_kwh",
            "historical_avg_price_per_kwh",
            "carbon_source",
            "price_signal_source",
            "pricing_status",
            "pricing_message",
            "pricing_source",
            "pricing_market",
            "pricing_region_code",
            "pricing_node",
            "forecast_region_requested",
            "forecast_region_used",
            "forecast_access_mode",
            "historical_region_used",
            "price_extension_status",
            "price_extension_message",
        ]
        if col in forecast_df.columns
    ]

    forecast_df = forecast_df[ordered_columns].copy()
    forecast_df = forecast_df.sort_values("timestamp").reset_index(drop=True)

    if forecast_df.empty:
        raise ValueError("Live carbon forecast table is empty.")

    return forecast_df


def get_forecast_table(
    forecast_mode: str,
    region: str,
    carbon_filepath: str | Path | None = None,
    price_filepath: str | Path | None = None,
    placeholder_price_per_kwh: float = 0.15,
    carbon_estimation_mode: str = "forecast_only",
    historical_days: int = 7,
    deadline: str | None = None,
) -> pd.DataFrame:
    if forecast_mode == "demo":
        if carbon_filepath is None or price_filepath is None:
            raise ValueError(
                "Demo mode requires carbon_filepath and price_filepath."
            )

        return build_forecast_table(carbon_filepath, price_filepath)

    if forecast_mode == "live_carbon":
        return build_live_carbon_forecast_table(
            region=region,
            placeholder_price_per_kwh=placeholder_price_per_kwh,
            carbon_estimation_mode=carbon_estimation_mode,
            historical_days=historical_days,
            deadline=deadline,
        )

    raise ValueError(
        "Invalid forecast_mode. Supported values are 'demo' and 'live_carbon'."
    )
