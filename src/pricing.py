from __future__ import annotations

from typing import Any

import pandas as pd

from src.price_adapters.base import PriceProviderError
from src.price_adapters.caiso import CaisoPricingError, fetch_caiso_day_ahead_prices
from src.price_adapters.ercot import ErcotPricingError, fetch_ercot_prices
from src.price_adapters.miso import MisoPricingError, fetch_miso_prices
from src.price_adapters.pjm import PjmPricingError, fetch_pjm_prices
from src.price_router import PricingRoute, UnsupportedPricingRegionError, resolve_pricing_route


class PricingUnavailableError(ValueError):
    """Raised when pricing cannot be fetched or routed for the requested region."""


def _fill_metadata_columns(aligned: pd.DataFrame) -> pd.DataFrame:
    metadata_columns = [
        "price_per_mwh",
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
    for column in metadata_columns:
        if column in aligned.columns:
            aligned[column] = aligned[column].ffill().bfill()
    return aligned


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
        if "price_per_mwh" in aligned.columns:
            aligned["price_per_mwh"] = aligned["price_per_mwh"].ffill().bfill()
    else:
        first_price_ts = prices["timestamp"].min()
        last_price_ts = prices["timestamp"].max()
        in_supported_range = aligned["timestamp"].between(first_price_ts, last_price_ts)
        aligned.loc[in_supported_range, "price_per_kwh"] = aligned.loc[in_supported_range, "price_per_kwh"].ffill().bfill()
        if "price_per_mwh" in aligned.columns:
            aligned.loc[in_supported_range, "price_per_mwh"] = aligned.loc[in_supported_range, "price_per_mwh"].ffill().bfill()
    aligned = _fill_metadata_columns(aligned)
    aligned["local_time"] = aligned["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    if aligned["price_per_kwh"].isna().all():
        raise PricingUnavailableError("Price alignment produced no usable price values.")

    print(
        "[PRICE ALIGN DEBUG] Alignment summary:",
        {
            "source_price_rows": len(prices),
            "target_intervals": len(aligned_targets),
            "aligned_intervals": len(aligned),
            "non_null_price_intervals": int(aligned["price_per_kwh"].notna().sum()),
            "alignment_method": "merge_asof_backward",
            "within_range_fill": "ffill+bfill",
            "carry_forward_beyond_last_known": carry_forward_beyond_last_known,
        },
    )

    ordered_columns = [
        column
        for column in [
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
        if column in aligned.columns
    ]
    return aligned[ordered_columns].copy()


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
        print(
            "[PRICE ROUTER] Selected route:",
            {
                "resolved_region": region_code,
                "provider_key": route.provider_key,
                "source_provider": route.source_provider,
                "source_market": route.source_market,
                "node_or_zone": route.node_or_zone,
                "coverage_note": route.coverage_note,
            },
        )
        raw_price_df = _fetch_price_series_for_route(
            route=route,
            start_time=start_time,
            end_time=end_time,
        )
    except (CaisoPricingError, ErcotPricingError, PjmPricingError, MisoPricingError, PriceProviderError) as exc:
        raise PricingUnavailableError(
            f"Pricing fetch failed for region '{region_code}' via provider '{route.source_provider}': {exc}"
        ) from exc
    except Exception as exc:
        raise PricingUnavailableError(
            f"Pricing fetch failed for region '{region_code}': {exc}"
        ) from exc

    return raw_price_df


def _fetch_price_series_for_route(
    *,
    route: PricingRoute,
    start_time: Any,
    end_time: Any,
) -> pd.DataFrame:
    provider_fetchers = {
        "caiso": lambda: fetch_caiso_day_ahead_prices(
            price_node=route.node_or_zone,
            region_code=route.region_code,
            start_time=start_time,
            end_time=end_time,
            market_run_id=route.source_market,
        ),
        "ercot": lambda: fetch_ercot_prices(
            region_code=route.region_code,
            node_or_zone=route.node_or_zone,
            start_time=start_time,
            end_time=end_time,
            market=route.source_market,
        ),
        "pjm": lambda: fetch_pjm_prices(
            region_code=route.region_code,
            node_or_zone=route.node_or_zone,
            start_time=start_time,
            end_time=end_time,
            market=route.source_market,
        ),
        "miso": lambda: fetch_miso_prices(
            region_code=route.region_code,
            node_or_zone=route.node_or_zone,
            start_time=start_time,
            end_time=end_time,
            market=route.source_market,
        ),
    }

    fetcher = provider_fetchers.get(route.provider_key)
    if fetcher is None:
        raise PricingUnavailableError(
            f"No provider fetcher is registered for pricing provider '{route.provider_key}'."
        )
    return fetcher()


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
    return aligned_df
