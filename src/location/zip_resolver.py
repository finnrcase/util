"""
ZIP code -> coordinate resolution for Util.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import pandas as pd
import pgeocode


@lru_cache(maxsize=4)
def _get_nominatim(country_code: str) -> pgeocode.Nominatim:
    return pgeocode.Nominatim(country_code)


def zip_to_coordinates(zip_code: str, country_code: str = "US") -> dict[str, Any]:
    """
    Resolve a ZIP code to approximate latitude/longitude using offline postal data.

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

    nomi = _get_nominatim(country_code)
    result = nomi.query_postal_code(zip_code)

    latitude = result.latitude
    longitude = result.longitude

    if pd.isna(latitude) or pd.isna(longitude):
        raise ValueError(f"Could not determine coordinates for ZIP code: {zip_code}")

    return {
        "zip_code": zip_code,
        "latitude": float(latitude),
        "longitude": float(longitude),
    }


def zip_to_place_label(zip_code: str, country_code: str = "US") -> str:
    """
    Resolve a ZIP code to a human-readable place label like
    "Los Angeles, CA" using offline postal data.

    Raises
    ------
    ValueError
        If the ZIP code cannot be resolved to a usable place label.
    """
    zip_code = str(zip_code).strip()

    nomi = _get_nominatim(country_code)
    result = nomi.query_postal_code(zip_code)

    place_name = getattr(result, "place_name", None)
    state_code = getattr(result, "state_code", None)

    if pd.isna(place_name) or not str(place_name).strip():
        raise ValueError(f"Could not determine place name for ZIP code: {zip_code}")

    place_label = str(place_name).strip()
    if not pd.isna(state_code) and str(state_code).strip():
        place_label = f"{place_label}, {str(state_code).strip()}"

    return place_label
