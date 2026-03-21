from __future__ import annotations

from dataclasses import dataclass


class UnsupportedPricingRegionError(ValueError):
    """Raised when a WattTime region does not have a supported pricing route."""


@dataclass(frozen=True)
class PricingRoute:
    adapter: str
    market: str
    price_node: str
    region_code: str
    coverage_note: str


CAISO_REGION_ROUTE_MAP: dict[str, PricingRoute] = {
    "CAISO_NORTH": PricingRoute(
        adapter="caiso",
        market="DAM",
        price_node="TH_NP15_GEN-APND",
        region_code="CAISO",
        coverage_note="Validated CAISO day-ahead path using TH_NP15_GEN-APND.",
    ),
    "CAISO": PricingRoute(
        adapter="caiso",
        market="DAM",
        price_node="TH_NP15_GEN-APND",
        region_code="CAISO",
        coverage_note="Validated CAISO day-ahead path using TH_NP15_GEN-APND.",
    ),
}


def resolve_pricing_route(region_code: str) -> PricingRoute:
    normalized = str(region_code).strip().upper()

    if normalized in CAISO_REGION_ROUTE_MAP:
        return CAISO_REGION_ROUTE_MAP[normalized]

    if normalized.startswith("CAISO"):
        return PricingRoute(
            adapter="caiso",
            market="DAM",
            price_node="TH_NP15_GEN-APND",
            region_code="CAISO",
            coverage_note="Default CAISO pricing route currently uses TH_NP15_GEN-APND.",
        )

    raise UnsupportedPricingRegionError(
        f"Electricity pricing is currently supported only for the California / CAISO path. "
        f"Region '{region_code}' does not have a pricing adapter yet."
    )
