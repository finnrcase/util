"""
Data loading utilities for Util.
"""

from pathlib import Path

import pandas as pd

from services.watttime_service import get_watttime_forecast, get_watttime_historical
from src.forecasting.carbon_blender import extend_forecast_with_history


def _normalize_timestamp_column(df: pd.DataFrame, column: str = "timestamp") -> pd.DataFrame:
    df = df.copy()
    df[column] = (
        pd.to_datetime(df[column], utc=True)
        .dt.tz_convert("America/Los_Angeles")
        .dt.tz_localize(None)
    )
    return df


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

    forecast_df = pd.merge(carbon_df, price_df, on="timestamp", how="inner")
    forecast_df = forecast_df.sort_values("timestamp").reset_index(drop=True)

    if forecast_df.empty:
        raise ValueError("Merged forecast table is empty. Check timestamp alignment.")

    return forecast_df


def _fetch_live_forecast_with_fallback(region: str):
    """
    Attempt to fetch WattTime forecast for requested region.
    If WattTime plan does not allow it, fall back to CAISO_NORTH.
    """
    try:
        carbon_df = get_watttime_forecast(region)
        region_used = region
        access_mode = "direct_region"

    except ValueError as exc:
        error_text = str(exc)

        if "forbidden (403)" in error_text or "INVALID_SCOPE" in error_text:

            fallback_region = "CAISO_NORTH"

            carbon_df = get_watttime_forecast(fallback_region)

            region_used = fallback_region
            access_mode = "preview_fallback"

        else:
            raise

    return carbon_df, region_used, access_mode


def _fetch_live_historical_with_fallback(region: str, days: int):
    """
    Attempt to fetch WattTime historical data for requested region.
    Falls back to CAISO_NORTH if needed.
    """
    try:
        historical_df = get_watttime_historical(region=region, days=days)
        region_used = region

    except ValueError as exc:
        error_text = str(exc)

        if "forbidden (403)" in error_text or "INVALID_SCOPE" in error_text:

            fallback_region = "CAISO_NORTH"

            historical_df = get_watttime_historical(
                region=fallback_region,
                days=days,
            )

            region_used = fallback_region

        else:
            raise

    return historical_df, region_used


def build_live_historical_export_table(
    region: str,
    days: int = 14,
) -> pd.DataFrame:
    """
    Fetch historical WattTime data for CSV export and normalize it to
    Util's local display timezone.
    """
    historical_df, historical_region_used = _fetch_live_historical_with_fallback(
        region,
        days,
    )
    historical_df = _normalize_timestamp_column(historical_df, "timestamp")
    historical_df["historical_region_used"] = historical_region_used
    return historical_df.sort_values("timestamp").reset_index(drop=True)


def build_live_carbon_forecast_table(
    region: str,
    placeholder_price_per_kwh: float = 0.15,
    carbon_estimation_mode: str = "forecast_only",
    historical_days: int = 7,
    deadline: str | None = None,
) -> pd.DataFrame:
    """
    Build a forecast table using live carbon forecast data from WattTime
    and a placeholder electricity price.

    carbon_estimation_mode:
    - forecast_only
    - forecast_plus_historical_expectation
    """

    requested_region = region

    carbon_df, forecast_region_used, forecast_access_mode = _fetch_live_forecast_with_fallback(
        requested_region
    )

    required_columns = {"timestamp", "carbon_g_per_kwh"}
    if not required_columns.issubset(carbon_df.columns):
        raise ValueError(
            f"WattTime forecast must contain columns: {required_columns}"
        )

    carbon_df = _normalize_timestamp_column(carbon_df, "timestamp")

    if carbon_estimation_mode == "forecast_plus_historical_expectation":

        if deadline is None:
            raise ValueError(
                "forecast_plus_historical_expectation mode requires deadline."
            )

        historical_df, historical_region_used = _fetch_live_historical_with_fallback(
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

    elif carbon_estimation_mode == "forecast_only":

        carbon_df["carbon_source"] = "live_forecast"

    else:

        raise ValueError(
            "carbon_estimation_mode must be either "
            "'forecast_only' or 'forecast_plus_historical_expectation'"
        )

    carbon_df["price_per_kwh"] = placeholder_price_per_kwh

    # metadata columns for transparency
    carbon_df["forecast_region_requested"] = requested_region
    carbon_df["forecast_region_used"] = forecast_region_used
    carbon_df["forecast_access_mode"] = forecast_access_mode

    ordered_columns = [
        col for col in [
            "timestamp",
            "carbon_g_per_kwh",
            "price_per_kwh",
            "historical_avg_carbon_g_per_kwh",
            "carbon_source",
            "forecast_region_requested",
            "forecast_region_used",
            "forecast_access_mode",
            "historical_region_used",
        ]
        if col in carbon_df.columns
    ]

    forecast_df = carbon_df[ordered_columns].copy()
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
    """
    Master forecast loader.

    Supported modes:
    - demo
    - live_carbon
    """

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
