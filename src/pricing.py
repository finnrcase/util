from __future__ import annotations

from typing import Any

import pandas as pd

from src.price_adapters.caiso import CaisoPricingError, fetch_caiso_day_ahead_prices
from src.region_router import UnsupportedPricingRegionError, resolve_pricing_route


class PricingUnavailableError(ValueError):
    """Raised when pricing cannot be fetched or routed for the requested region."""


def _align_prices_to_timestamps(
    price_df: pd.DataFrame,
    target_timestamps: pd.Series,
) -> pd.DataFrame:
    aligned_targets = pd.DataFrame(
        {"timestamp": pd.to_datetime(target_timestamps, errors="coerce")}
    ).dropna(subset=["timestamp"]).sort_values("timestamp")

    if aligned_targets.empty:
        raise PricingUnavailableError("No target timestamps were provided for price alignment.")

    prices = price_df.copy()
    prices["timestamp"] = pd.to_datetime(prices["timestamp"], errors="coerce")
    prices = prices.dropna(subset=["timestamp"]).sort_values("timestamp")

    aligned = pd.merge_asof(
        aligned_targets,
        prices,
        on="timestamp",
        direction="backward",
    )
    aligned["price_per_kwh"] = aligned["price_per_kwh"].ffill().bfill()
    aligned["source"] = aligned["source"].ffill().bfill()
    aligned["region_code"] = aligned["region_code"].ffill().bfill()
    aligned["local_time"] = aligned["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    if aligned["price_per_kwh"].isna().all():
        raise PricingUnavailableError("Price alignment produced no usable price values.")

    return aligned[["timestamp", "local_time", "price_per_kwh", "source", "region_code"]]


def get_normalized_price_series(
    *,
    region_code: str,
    target_timestamps: pd.Series,
) -> pd.DataFrame:
    try:
        route = resolve_pricing_route(region_code)
    except UnsupportedPricingRegionError as exc:
        raise PricingUnavailableError(str(exc)) from exc

    try:
        if route.adapter == "caiso":
            raw_price_df = fetch_caiso_day_ahead_prices(
                price_node=route.price_node,
                region_code=route.region_code,
                start_time=target_timestamps.min(),
                end_time=target_timestamps.max(),
                market_run_id=route.market,
            )
        else:
            raise PricingUnavailableError(
                f"Adapter '{route.adapter}' is not implemented yet."
            )
    except CaisoPricingError as exc:
        raise PricingUnavailableError(
            f"CAISO pricing fetch failed for region '{region_code}': {exc}"
        ) from exc
    except Exception as exc:
        raise PricingUnavailableError(
            f"Pricing fetch failed for region '{region_code}': {exc}"
        ) from exc

    aligned_df = _align_prices_to_timestamps(raw_price_df, target_timestamps=target_timestamps)
    if "price_node" in raw_price_df.columns:
        aligned_df["price_node"] = raw_price_df["price_node"].dropna().iloc[0]
    return aligned_df
