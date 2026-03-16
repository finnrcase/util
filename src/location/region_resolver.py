"""
Coordinate -> WattTime region resolution for Util.
"""

from __future__ import annotations

from typing import Any

from services.watttime_service import get_region_from_loc


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
    response = get_region_from_loc(latitude=latitude, longitude=longitude)

    region = response.get("region")
    region_full_name = response.get("region_full_name")
    signal_type = response.get("signal_type")

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