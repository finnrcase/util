"""
Coordinate -> WattTime region resolution for Util.
"""

from __future__ import annotations

from typing import Any

from services.watttime_service import get_ba_from_loc


def coordinates_to_watttime_region(
    latitude: float,
    longitude: float,
) -> dict[str, Any]:
    """
    Resolve latitude/longitude to a WattTime balancing authority / region.

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
        - watttime_name
        - watttime_id

    Raises
    ------
    ValueError
        If WattTime region lookup fails or returns incomplete data.
    """
    response = get_ba_from_loc(latitude=latitude, longitude=longitude)

    region = response.get("abbrev")
    name = response.get("name")
    region_id = response.get("id")

    if not region:
        raise ValueError(
            f"WattTime region lookup failed for coordinates: ({latitude}, {longitude})"
        )

    return {
        "latitude": latitude,
        "longitude": longitude,
        "watttime_region": region,
        "watttime_name": name,
        "watttime_id": region_id,
        "raw_response": response,
    }