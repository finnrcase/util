from __future__ import annotations

from typing import Any

import pandas as pd


NORMALIZED_PRICE_COLUMNS = [
    "timestamp",
    "local_time",
    "price_per_mwh",
    "price_per_kwh",
    "source_market",
    "source_provider",
    "node_or_zone",
    "interval_minutes",
    "price_type",
    "is_forecast_or_historical",
    "is_live_market_data",
    "source",
    "region_code",
    "price_node",
]


class PriceProviderError(ValueError):
    """Raised when a price provider cannot fetch or normalize data."""


class PriceProviderNotImplementedError(PriceProviderError):
    """Raised when a routed provider is recognized but not yet implemented."""


def finalize_normalized_price_frame(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], errors="coerce")
    normalized = normalized.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    if "price_per_mwh" not in normalized.columns and "price_per_kwh" in normalized.columns:
        normalized["price_per_mwh"] = pd.to_numeric(normalized["price_per_kwh"], errors="coerce") * 1000.0

    if "price_per_kwh" not in normalized.columns and "price_per_mwh" in normalized.columns:
        normalized["price_per_kwh"] = pd.to_numeric(normalized["price_per_mwh"], errors="coerce") / 1000.0

    normalized["price_per_mwh"] = pd.to_numeric(normalized["price_per_mwh"], errors="coerce")
    normalized["price_per_kwh"] = pd.to_numeric(normalized["price_per_kwh"], errors="coerce")

    if "local_time" not in normalized.columns:
        normalized["local_time"] = normalized["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    if "interval_minutes" not in normalized.columns:
        if len(normalized) >= 2:
            interval_minutes = (
                normalized["timestamp"].sort_values().diff().dropna().dt.total_seconds().median() / 60.0
            )
            normalized["interval_minutes"] = float(interval_minutes)
        else:
            normalized["interval_minutes"] = pd.NA

    if "price_type" not in normalized.columns:
        normalized["price_type"] = "day_ahead_lmp"

    if "is_live_market_data" not in normalized.columns:
        normalized["is_live_market_data"] = True

    if "source" not in normalized.columns and "source_provider" in normalized.columns:
        normalized["source"] = normalized["source_provider"]

    if "price_node" not in normalized.columns and "node_or_zone" in normalized.columns:
        normalized["price_node"] = normalized["node_or_zone"]

    missing_columns = [column for column in ["timestamp", "price_per_mwh", "price_per_kwh"] if column not in normalized.columns]
    if missing_columns:
        raise PriceProviderError(f"Normalized price dataframe is missing required columns: {missing_columns}")

    if normalized["price_per_kwh"].isna().all():
        raise PriceProviderError("Normalized price dataframe contained no usable price values.")

    ordered_columns = [column for column in NORMALIZED_PRICE_COLUMNS if column in normalized.columns]
    return normalized[ordered_columns].copy()


def build_unavailable_price_message(*, provider: str, region_code: str, details: str) -> str:
    return (
        f"Live electricity pricing is not available for region '{region_code}' "
        f"through provider '{provider}'. {details}"
    )
