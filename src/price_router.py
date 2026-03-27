from __future__ import annotations

from dataclasses import dataclass
import re


class UnsupportedPricingRegionError(ValueError):
    """Raised when a resolved power region does not have a supported pricing route."""


@dataclass(frozen=True)
class PricingRoute:
    provider_key: str
    source_market: str
    source_provider: str
    node_or_zone: str
    region_code: str
    coverage_note: str
    is_supported: bool = True


EXACT_REGION_ROUTE_MAP: dict[str, PricingRoute] = {
    "CAISO_NORTH": PricingRoute(
        provider_key="caiso",
        source_market="DAM",
        source_provider="CAISO",
        node_or_zone="TH_NP15_GEN-APND",
        region_code="CAISO_NORTH",
        coverage_note="Validated CAISO north day-ahead route via TH_NP15_GEN-APND.",
    ),
    "CAISO": PricingRoute(
        provider_key="caiso",
        source_market="DAM",
        source_provider="CAISO",
        node_or_zone="TH_NP15_GEN-APND",
        region_code="CAISO",
        coverage_note="Validated CAISO day-ahead route via TH_NP15_GEN-APND.",
    ),
    "LDWP": PricingRoute(
        provider_key="caiso",
        source_market="DAM",
        source_provider="CAISO",
        node_or_zone="TH_SP15_GEN-APND",
        region_code="LDWP",
        coverage_note="LDWP routed to the CAISO south proxy node TH_SP15_GEN-APND.",
    ),
    "LADWP": PricingRoute(
        provider_key="caiso",
        source_market="DAM",
        source_provider="CAISO",
        node_or_zone="TH_SP15_GEN-APND",
        region_code="LDWP",
        coverage_note="LADWP mapped to the CAISO south proxy node TH_SP15_GEN-APND.",
    ),
    "PGE": PricingRoute(
        provider_key="caiso",
        source_market="DAM",
        source_provider="CAISO",
        node_or_zone="TH_NP15_GEN-APND",
        region_code="PGE",
        coverage_note="PG&E mapped to the CAISO north proxy node TH_NP15_GEN-APND.",
    ),
    "PGECALIFORNIA": PricingRoute(
        provider_key="caiso",
        source_market="DAM",
        source_provider="CAISO",
        node_or_zone="TH_NP15_GEN-APND",
        region_code="PGE",
        coverage_note="PG&E mapped to the CAISO north proxy node TH_NP15_GEN-APND.",
    ),
    "SCE": PricingRoute(
        provider_key="caiso",
        source_market="DAM",
        source_provider="CAISO",
        node_or_zone="TH_SP15_GEN-APND",
        region_code="SCE",
        coverage_note="SCE mapped to the CAISO south proxy node TH_SP15_GEN-APND.",
    ),
    "SDGE": PricingRoute(
        provider_key="caiso",
        source_market="DAM",
        source_provider="CAISO",
        node_or_zone="TH_SP15_GEN-APND",
        region_code="SDGE",
        coverage_note="SDG&E mapped to the CAISO south proxy node TH_SP15_GEN-APND.",
    ),
    "ERCOT_EASTTX": PricingRoute(
        provider_key="ercot",
        source_market="DAM",
        source_provider="ERCOT",
        node_or_zone="LZ_HOUSTON",
        region_code="ERCOT_EASTTX",
        coverage_note="ERCOT_EASTTX mapped to the ERCOT Houston load-zone day-ahead settlement point LZ_HOUSTON.",
    ),
    "PJM_CHICAGO": PricingRoute(
        provider_key="pjm",
        source_market="DAY_AHEAD",
        source_provider="PJM",
        node_or_zone="COMED",
        region_code="PJM_CHICAGO",
        coverage_note="PJM_CHICAGO mapped to the PJM COMED zonal day-ahead LMP feed.",
    ),
}

PREFIX_REGION_ROUTE_MAP: dict[str, PricingRoute] = {
    "CAISO": PricingRoute(
        provider_key="caiso",
        source_market="DAM",
        source_provider="CAISO",
        node_or_zone="TH_NP15_GEN-APND",
        region_code="CAISO",
        coverage_note="Default CAISO pricing route currently uses TH_NP15_GEN-APND.",
    ),
    "ERCOT": PricingRoute(
        provider_key="ercot",
        source_market="DAM",
        source_provider="ERCOT",
        node_or_zone="ERCOT_DEFAULT_ZONE",
        region_code="ERCOT",
        coverage_note="ERCOT provider is live, but this generic ERCOT route still needs an explicit load-zone or hub mapping.",
    ),
    "PJM": PricingRoute(
        provider_key="pjm",
        source_market="DAY_AHEAD",
        source_provider="PJM",
        node_or_zone="PJM_DEFAULT_ZONE",
        region_code="PJM",
        coverage_note="PJM default pricing route uses a placeholder zone until a region-specific zonal mapping is configured.",
    ),
    "MISO": PricingRoute(
        provider_key="miso",
        source_market="DAY_AHEAD",
        source_provider="MISO",
        node_or_zone="MISO_DEFAULT_ZONE",
        region_code="MISO",
        coverage_note="MISO routing scaffold is present, but the provider implementation is not yet live.",
    ),
}

CALIFORNIA_REGION_ALIASES: dict[str, str] = {
    "LOSANGELESDEPARTMENTOFWATERANDPOWER": "LDWP",
    "LACITYOFLOSANGELESDEPARTMENTOFWATERANDPOWER": "LDWP",
    "LADWP": "LADWP",
    "LDWP": "LDWP",
    "PACIFICGASANDELECTRIC": "PGE",
    "PACIFICGASELECTRIC": "PGE",
    "PGE": "PGE",
    "PGECALIFORNIA": "PGE",
    "SOUTHERNCALIFORNIAEDISON": "SCE",
    "SCE": "SCE",
    "SANDIEGOGASANDELECTRIC": "SDGE",
    "SDGE": "SDGE",
}


def _normalize_region_key(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(value).strip().upper())


def resolve_pricing_route(region_code: str) -> PricingRoute:
    normalized = str(region_code).strip().upper()
    normalized_key = _normalize_region_key(region_code)

    alias_region = CALIFORNIA_REGION_ALIASES.get(normalized_key)
    if alias_region and alias_region in EXACT_REGION_ROUTE_MAP:
        return EXACT_REGION_ROUTE_MAP[alias_region]

    if normalized in EXACT_REGION_ROUTE_MAP:
        return EXACT_REGION_ROUTE_MAP[normalized]

    for prefix, route in PREFIX_REGION_ROUTE_MAP.items():
        if normalized.startswith(prefix):
            return route

    raise UnsupportedPricingRegionError(
        f"No live price provider route is configured yet for resolved region '{region_code}'."
    )
