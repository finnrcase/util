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