from __future__ import annotations

"""
Backward-compatible pricing router shim.

This module now delegates to src.price_router so older imports still resolve
through the single active region->market routing layer.
"""

from src.price_router import PricingRoute, UnsupportedPricingRegionError, resolve_pricing_route

__all__ = ["PricingRoute", "UnsupportedPricingRegionError", "resolve_pricing_route"]
