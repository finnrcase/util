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
    ZIP code -> latitude/longitude -> WattTime balancing authority

    Parameters
    ----------
    zip_code : str
        User ZIP code.

    Returns
    -------
    dict
        Dictionary containing:
        - zip_code
        - latitude
        - longitude
        - watttime_region
        - watttime_name
        - watttime_id
    """
    coordinate_info = zip_to_coordinates(zip_code)
    region_info = coordinates_to_watttime_region(
        latitude=coordinate_info["latitude"],
        longitude=coordinate_info["longitude"],
    )

    return {
        "zip_code": coordinate_info["zip_code"],
        "latitude": coordinate_info["latitude"],
        "longitude": coordinate_info["longitude"],
        "watttime_region": region_info["watttime_region"],
        "watttime_name": region_info["watttime_name"],
        "watttime_id": region_info["watttime_id"],
        "raw_response": region_info["raw_response"],
    }