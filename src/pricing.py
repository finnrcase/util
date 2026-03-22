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
    *,
    carry_forward_beyond_last_known: bool = True,
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
    if carry_forward_beyond_last_known:
        aligned["price_per_kwh"] = aligned["price_per_kwh"].ffill().bfill()
    else:
        first_price_ts = prices["timestamp"].min()
        last_price_ts = prices["timestamp"].max()
        in_supported_range = aligned["timestamp"].between(first_price_ts, last_price_ts)
        aligned.loc[in_supported_range, "price_per_kwh"] = aligned.loc[in_supported_range, "price_per_kwh"].ffill().bfill()
    aligned["source"] = aligned["source"].ffill().bfill()
    aligned["region_code"] = aligned["region_code"].ffill().bfill()
    aligned["local_time"] = aligned["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    if aligned["price_per_kwh"].isna().all():
        raise PricingUnavailableError("Price alignment produced no usable price values.")

    return aligned[["timestamp", "local_time", "price_per_kwh", "source", "region_code"]]


def align_price_series(
    *,
    price_df: pd.DataFrame,
    target_timestamps: pd.Series,
    carry_forward_beyond_last_known: bool = True,
) -> pd.DataFrame:
    return _align_prices_to_timestamps(
        price_df,
        target_timestamps=target_timestamps,
        carry_forward_beyond_last_known=carry_forward_beyond_last_known,
    )


def get_price_series(
    *,
    region_code: str,
    start_time: Any,
    end_time: Any,
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
                start_time=start_time,
                end_time=end_time,
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

    return raw_price_df


def get_normalized_price_series(
    *,
    region_code: str,
    target_timestamps: pd.Series,
    carry_forward_beyond_last_known: bool = True,
) -> pd.DataFrame:
    raw_price_df = get_price_series(
        region_code=region_code,
        start_time=target_timestamps.min(),
        end_time=target_timestamps.max(),
    )
    aligned_df = align_price_series(
        price_df=raw_price_df,
        target_timestamps=target_timestamps,
        carry_forward_beyond_last_known=carry_forward_beyond_last_known,
    )
    if "price_node" in raw_price_df.columns:
        aligned_df["price_node"] = raw_price_df["price_node"].dropna().iloc[0]
    return aligned_df
