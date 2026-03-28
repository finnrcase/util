"""
ZIP code -> coordinate resolution for Util.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from functools import lru_cache
import logging
from typing import Any

import pandas as pd
import pgeocode


ZIP_RESOLUTION_TIMEOUT_SECONDS = 12
_zip_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="util_zip_lookup")
zip_logger = logging.getLogger("uvicorn.error")


@lru_cache(maxsize=4)
def _get_nominatim(country_code: str) -> pgeocode.Nominatim:
    zip_logger.info("Util ZIP lookup: initializing pgeocode dataset for country=%s", country_code)
    return pgeocode.Nominatim(country_code)


def _query_postal_code(zip_code: str, country_code: str) -> Any:
    nomi = _get_nominatim(country_code)
    return nomi.query_postal_code(zip_code)


def _query_postal_code_with_timeout(zip_code: str, country_code: str) -> Any:
    future = _zip_executor.submit(_query_postal_code, zip_code, country_code)
    try:
        return future.result(timeout=ZIP_RESOLUTION_TIMEOUT_SECONDS)
    except FutureTimeoutError as exc:
        future.cancel()
        raise TimeoutError(
            f"ZIP coordinate lookup timed out after {ZIP_RESOLUTION_TIMEOUT_SECONDS} seconds for ZIP code {zip_code}."
        ) from exc


def zip_to_coordinates(zip_code: str, country_code: str = "US") -> dict[str, Any]:
    """
    Resolve a ZIP code to approximate latitude/longitude using postal data.

    Parameters
    ----------
    zip_code : str
        ZIP code provided by the user.
    country_code : str
        Postal-country code for pgeocode. Defaults to "US".

    Returns
    -------
    dict
        Dictionary containing:
        - zip_code
        - latitude
        - longitude

    Raises
    ------
    ValueError
        If the ZIP code cannot be resolved.
    """
    zip_code = str(zip_code).strip()
    zip_logger.info("Util ZIP lookup: starting coordinate resolution for zip=%s", zip_code)

    result = _query_postal_code_with_timeout(zip_code, country_code)

    latitude = result.latitude
    longitude = result.longitude

    if pd.isna(latitude) or pd.isna(longitude):
        raise ValueError(f"Could not determine coordinates for ZIP code: {zip_code}")

    resolved = {
        "zip_code": zip_code,
        "latitude": float(latitude),
        "longitude": float(longitude),
    }
    zip_logger.info(
        "Util ZIP lookup: resolved zip=%s latitude=%s longitude=%s",
        zip_code,
        resolved["latitude"],
        resolved["longitude"],
    )
    return resolved


def zip_to_place_label(zip_code: str, country_code: str = "US") -> str:
    """
    Resolve a ZIP code to a human-readable place label like
    "Los Angeles, CA" using postal data.

    Raises
    ------
    ValueError
        If the ZIP code cannot be resolved to a usable place label.
    """
    zip_code = str(zip_code).strip()

    result = _query_postal_code_with_timeout(zip_code, country_code)

    place_name = getattr(result, "place_name", None)
    state_code = getattr(result, "state_code", None)

    if pd.isna(place_name) or not str(place_name).strip():
        raise ValueError(f"Could not determine place name for ZIP code: {zip_code}")

    place_label = str(place_name).strip()
    if not pd.isna(state_code) and str(state_code).strip():
        place_label = f"{place_label}, {str(state_code).strip()}"

    return place_label
