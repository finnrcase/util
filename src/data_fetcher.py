"""
Data loading utilities for Util.
"""

from pathlib import Path

import pandas as pd

from services.watttime_service import get_watttime_forecast, get_watttime_historical
from src.forecasting.carbon_blender import blend_live_forecast_with_history


def _normalize_timestamp_column(df: pd.DataFrame, column: str = "timestamp") -> pd.DataFrame:
    df = df.copy()
    df[column] = pd.to_datetime(df[column])

    if getattr(df[column].dt, "tz", None) is not None:
        df[column] = df[column].dt.tz_localize(None)

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


def build_live_carbon_forecast_table(
    region: str,
    placeholder_price_per_kwh: float = 0.15,
    carbon_estimation_mode: str = "live_only",
    historical_days: int = 7,
    live_weight: float = 0.7,
    history_weight: float = 0.3,
) -> pd.DataFrame:
    """
    Build a forecast table using live carbon forecast data from WattTime
    and a placeholder electricity price.

    carbon_estimation_mode:
    - live_only
    - live_plus_history
    """
    _ = region
    watttime_region = "CAISO_NORTH"

    carbon_df = get_watttime_forecast(watttime_region)

    required_columns = {"timestamp", "carbon_g_per_kwh"}
    if not required_columns.issubset(carbon_df.columns):
        raise ValueError(
            f"WattTime forecast must contain columns: {required_columns}"
        )

    carbon_df = _normalize_timestamp_column(carbon_df, "timestamp")

    if carbon_estimation_mode == "live_plus_history":
        historical_df = get_watttime_historical(
            region=watttime_region,
            days=historical_days,
        )
        historical_df = _normalize_timestamp_column(historical_df, "timestamp")

        carbon_df = blend_live_forecast_with_history(
            live_forecast_df=carbon_df,
            historical_df=historical_df,
            live_weight=live_weight,
            history_weight=history_weight,
        )

    elif carbon_estimation_mode != "live_only":
        raise ValueError(
            "carbon_estimation_mode must be either 'live_only' or 'live_plus_history'"
        )

    carbon_df["price_per_kwh"] = placeholder_price_per_kwh

    forecast_columns = ["timestamp", "carbon_g_per_kwh", "price_per_kwh"]
    extra_columns = [
        col for col in [
            "raw_live_carbon_g_per_kwh",
            "historical_avg_carbon_g_per_kwh",
        ]
        if col in carbon_df.columns
    ]

    forecast_df = carbon_df[forecast_columns + extra_columns]
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
    carbon_estimation_mode: str = "live_only",
    historical_days: int = 7,
    live_weight: float = 0.7,
    history_weight: float = 0.3,
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
            live_weight=live_weight,
            history_weight=history_weight,
        )

    raise ValueError(
        "Invalid forecast_mode. Supported values are 'demo' and 'live_carbon'."
    )