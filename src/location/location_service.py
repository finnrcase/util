"""
High-level location resolution service for Util.
"""

from __future__ import annotations

from typing import Any

from src.location.region_resolver import coordinates_to_watttime_region
from src.location.zip_resolver import zip_to_coordinates


def resolve_zip_to_watttime_region(zip_code: str) -> dict[str, Any]:
    """
    Resolve a ZIP code all the way to a WattTime region.

    Flow:
    ZIP code -> latitude/longitude -> WattTime region
    """
    print(f"[LOCATION DEBUG] ZIP received: {zip_code}")
    coordinate_info = zip_to_coordinates(zip_code)
    print(
        "[LOCATION DEBUG] ZIP resolved to coordinates:",
        {
            "zip_code": coordinate_info["zip_code"],
            "latitude": coordinate_info["latitude"],
            "longitude": coordinate_info["longitude"],
        },
    )
    region_info = coordinates_to_watttime_region(
        latitude=coordinate_info["latitude"],
        longitude=coordinate_info["longitude"],
    )
    print(
        "[LOCATION DEBUG] Coordinates resolved to WattTime region:",
        {
            "region": region_info["watttime_region"],
            "region_full_name": region_info["watttime_region_full_name"],
            "signal_type": region_info["signal_type"],
        },
    )

    return {
        "zip_code": coordinate_info["zip_code"],
        "latitude": coordinate_info["latitude"],
        "longitude": coordinate_info["longitude"],
        "watttime_region": region_info["watttime_region"],
        "watttime_region_full_name": region_info["watttime_region_full_name"],
        "signal_type": region_info["signal_type"],
        "raw_response": region_info["raw_response"],
    }
