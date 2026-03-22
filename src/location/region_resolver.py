"""
Coordinate -> WattTime region resolution for Util.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from services.watttime_service import get_region_from_loc


@lru_cache(maxsize=32)
def _get_region_from_loc_cached(
    latitude: float,
    longitude: float,
) -> tuple[str, str | None, str | None, dict[str, Any]]:
    response = get_region_from_loc(latitude=latitude, longitude=longitude)
    return (
        response.get("region"),
        response.get("region_full_name"),
        response.get("signal_type"),
        response,
    )


def coordinates_to_watttime_region(
    latitude: float,
    longitude: float,
) -> dict[str, Any]:
    """
    Resolve latitude/longitude to a WattTime region.

    Parameters
    ----------
    latitude : float
        Latitude coordinate.
    longitude : float
        Longitude coordinate.

    Returns
    -------
    dict
        Dictionary containing:
        - latitude
        - longitude
        - watttime_region
        - watttime_region_full_name
        - signal_type
        - raw_response

    Raises
    ------
    ValueError
        If WattTime region lookup fails or returns incomplete data.
    """
    region, region_full_name, signal_type, response = _get_region_from_loc_cached(
        latitude=latitude,
        longitude=longitude,
    )

    if not region:
        raise ValueError(
            f"WattTime region lookup failed for coordinates: ({latitude}, {longitude})"
        )

    return {
        "latitude": latitude,
        "longitude": longitude,
        "watttime_region": region,
        "watttime_region_full_name": region_full_name,
        "signal_type": signal_type,
        "raw_response": response,
    }
