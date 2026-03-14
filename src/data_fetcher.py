"""
Data loading utilities for Util.

DEVELOPMENT NOTE:
This module currently loads placeholder CSV data from the data/raw folder.

TODO:
Replace placeholder CSV loading with real API/service-based data loading from:
- electricityMap
- WattTime
- ISO market data
"""

from pathlib import Path

import pandas as pd

from services.watttime_service import get_watttime_forecast


def load_carbon_forecast(filepath: str | Path) -> pd.DataFrame:
    """
    Load hourly carbon intensity forecast data from CSV.

    Expected columns:
    - timestamp
    - carbon_g_per_kwh
    """
    df = pd.read_csv(filepath)
    required_columns = {"timestamp", "carbon_g_per_kwh"}

    if not required_columns.issubset(df.columns):
        raise ValueError(
            f"Carbon forecast file must contain columns: {required_columns}"
        )

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def load_price_forecast(filepath: str | Path) -> pd.DataFrame:
    """
    Load hourly electricity price forecast data from CSV.

    Expected columns:
    - timestamp
    - price_per_kwh
    """
    df = pd.read_csv(filepath)
    required_columns = {"timestamp", "price_per_kwh"}

    if not required_columns.issubset(df.columns):
        raise ValueError(
            f"Price forecast file must contain columns: {required_columns}"
        )

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def build_forecast_table(
    carbon_filepath: str | Path,
    price_filepath: str | Path,
) -> pd.DataFrame:
    """
    Load carbon and price forecast CSVs and merge them into one table.
    """
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
) -> pd.DataFrame:
    """
    Build a forecast table using live carbon forecast data from WattTime
    and a placeholder electricity price.

    Returns columns:
    - timestamp
    - carbon_g_per_kwh
    - price_per_kwh
    """
    carbon_df = get_watttime_forecast(region)

    required_columns = {"timestamp", "carbon_g_per_kwh"}
    if not required_columns.issubset(carbon_df.columns):
        raise ValueError(
            f"WattTime forecast must contain columns: {required_columns}"
        )

    carbon_df = carbon_df.copy()
    carbon_df["timestamp"] = pd.to_datetime(carbon_df["timestamp"])
    carbon_df["price_per_kwh"] = placeholder_price_per_kwh

    forecast_df = carbon_df[["timestamp", "carbon_g_per_kwh", "price_per_kwh"]]
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
) -> pd.DataFrame:
    """
    Master forecast loader.

    Supported modes:
    - demo: load local CSV carbon + price forecast
    - live_carbon: use live WattTime carbon forecast + placeholder price
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
        )

    raise ValueError(
        "Invalid forecast_mode. Supported values are 'demo' and 'live_carbon'."
    )