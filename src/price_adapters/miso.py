from __future__ import annotations

from typing import Any

from src.price_adapters.base import PriceProviderNotImplementedError, build_unavailable_price_message


class MisoPricingError(PriceProviderNotImplementedError):
    """Raised when MISO pricing is routed but not yet implemented."""


def fetch_miso_prices(
    *,
    region_code: str,
    node_or_zone: str,
    start_time: Any,
    end_time: Any,
    market: str,
) -> None:
    raise MisoPricingError(
        build_unavailable_price_message(
            provider="MISO",
            region_code=region_code,
            details=(
                f"Routed zone/node '{node_or_zone}' for market '{market}', but the MISO provider adapter is "
                "currently scaffolded only."
            ),
        )
    )
