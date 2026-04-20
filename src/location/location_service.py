"""
High-level location resolution service for Util.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from src.location.region_resolver import coordinates_to_watttime_region
from src.location.zip_resolver import zip_to_coordinates


location_logger = logging.getLogger("uvicorn.error")


def resolve_zip_to_watttime_region(zip_code: str) -> dict[str, Any]:
    started_at = time.perf_counter()
    location_logger.info("Util location: zip->coordinates start zip=%s", zip_code)
    try:
        coordinate_info = zip_to_coordinates(zip_code)
        location_logger.info(
            "Util location: zip->coordinates success zip=%s lat=%s lon=%s source=%s retry_used=%s lookup_elapsed_ms=%.1f",
            coordinate_info["zip_code"],
            coordinate_info["latitude"],
            coordinate_info["longitude"],
            coordinate_info.get("_lookup_source", "unknown"),
            coordinate_info.get("_retry_used", False),
            float(coordinate_info.get("_lookup_duration_ms", 0.0)),
        )
    except TimeoutError:
        location_logger.exception(
            "Util location: zip->coordinates timeout zip=%s total_elapsed_ms=%.1f",
            zip_code,
            (time.perf_counter() - started_at) * 1000.0,
        )
        raise
    except Exception:
        location_logger.exception(
            "Util location: zip->coordinates failure zip=%s total_elapsed_ms=%.1f",
            zip_code,
            (time.perf_counter() - started_at) * 1000.0,
        )
        raise

    region_started_at = time.perf_counter()
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
        "Util location: coordinates->watttime region success region=%s signal_type=%s elapsed_ms=%.1f total_elapsed_ms=%.1f",
        region_info["watttime_region"],
        region_info["signal_type_used"],
        (time.perf_counter() - region_started_at) * 1000.0,
        (time.perf_counter() - started_at) * 1000.0,
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
