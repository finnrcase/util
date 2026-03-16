"""
ZIP code -> coordinate resolution for Util.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pgeocode


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

    nomi = pgeocode.Nominatim(country_code)
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