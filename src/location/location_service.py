"""
High-level location resolution service for Util.
"""

from __future__ import annotations

import logging
from typing import Any

from src.location.region_resolver import coordinates_to_watttime_region
from src.location.zip_resolver import zip_to_coordinates


location_logger = logging.getLogger("uvicorn.error")


def resolve_zip_to_watttime_region(zip_code: str) -> dict[str, Any]:
    """
    Resolve a ZIP code all the way to a WattTime region.

    Flow:
    ZIP code -> latitude/longitude -> WattTime region
    """
    location_logger.info("Util location: zip->coordinates start zip=%s", zip_code)
    coordinate_info = zip_to_coordinates(zip_code)
    location_logger.info(
        "Util location: zip->coordinates success zip=%s lat=%s lon=%s",
        coordinate_info["zip_code"],
        coordinate_info["latitude"],
        coordinate_info["longitude"],
    )

    location_logger.info(
        "Util location: coordinates->watttime region start lat=%s lon=%s",
        coordinate_info["latitude"],
        coordinate_info["longitude"],
    )
    region_info = coordinates_to_watttime_region(
        latitude=coordinate_info["latitude"],
        longitude=coordinate_info["longitude"],
    )
    location_logger.info(
        "Util location: coordinates->watttime region success region=%s signal_type=%s",
        region_info["watttime_region"],
        region_info["signal_type_used"],
    )

    return {
        "zip_code": coordinate_info["zip_code"],
        "latitude": coordinate_info["latitude"],
        "longitude": coordinate_info["longitude"],
        "watttime_region": region_info["watttime_region"],
        "watttime_region_full_name": region_info["watttime_region_full_name"],
        "signal_type_used": region_info["signal_type_used"],
        "location_lookup_status": "success",
        "raw_response": region_info["raw_response"],
    }
