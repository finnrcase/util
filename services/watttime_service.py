import os
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# WattTime API configuration
# ============================================================
# CURRENT PROTOTYPE VERSION
# -------------------------
# We are integrating live carbon forecast data first.
#
# For now, we are intentionally hardcoding:
#   region = "CAISO_NORTH"
#
# Why:
# - this gets live carbon forecasts working immediately
# - it avoids getting blocked on full ZIP -> lat/lon -> BA mapping
# - it keeps the MVP stable while we validate the product
#
# FUTURE PAID / FULLER API VERSION
# --------------------------------
# Replace the temporary hardcoded region flow with:
#
#   ZIP code
#   -> lat/lon geocoding
#   -> WattTime region / BA lookup
#   -> region-specific carbon forecast
#
# This file is the main place to upgrade that logic later.
# ============================================================

USERNAME = os.getenv("WATTTIME_USERNAME")
PASSWORD = os.getenv("WATTTIME_PASSWORD")

LOGIN_URL = "https://api2.watttime.org/v2/login"
FORECAST_URL = "https://api.watttime.org/v3/forecast"


def get_token():
    """
    Log in to WattTime and return a bearer token.
    """
    response = requests.get(
        LOGIN_URL,
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        timeout=30
    )
    response.raise_for_status()
    return response.json()["token"]


def get_forecast(region="CAISO_NORTH", signal_type="co2_moer"):
    """
    Fetch live carbon forecast data from WattTime.

    Current prototype behavior:
    - Uses a hardcoded default region of CAISO_NORTH
    - Uses signal_type='co2_moer'

    Future production behavior:
    - region should come from ZIP -> lat/lon -> region lookup
    """
    token = get_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    params = {
        "region": region,
        "signal_type": signal_type,
    }

    response = requests.get(
        FORECAST_URL,
        headers=headers,
        params=params,
        timeout=30
    )

    print("FORECAST STATUS:", response.status_code)
    print("FORECAST CONTENT TYPE:", response.headers.get("content-type"))
    print("FORECAST URL:", response.url)
    print("FORECAST RESPONSE PREVIEW:", response.text[:500])

    response.raise_for_status()

    if "application/json" not in response.headers.get("content-type", ""):
        raise ValueError("Forecast endpoint did not return JSON.")

    return response.json()


def forecast_to_dataframe(forecast_json):
    """
    Convert WattTime forecast JSON into the standard Util dataframe format.

    Expected normalized columns:
    - timestamp
    - carbon_g_per_kwh
    """

    # WattTime v3 forecast returns:
    # {
    #   "data": [
    #       {"point_time": "...", "value": ...},
    #       ...
    #   ]
    # }
    #
    # So we unwrap the "data" list first.

    if isinstance(forecast_json, dict):
        if "data" not in forecast_json:
            raise ValueError(
                f"Expected forecast JSON to contain a 'data' key. "
                f"Keys returned: {list(forecast_json.keys())}"
            )
        rows = forecast_json["data"]
    elif isinstance(forecast_json, list):
        rows = forecast_json
    else:
        raise ValueError(
            f"Unexpected forecast_json type: {type(forecast_json)}"
        )

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("Forecast response was empty.")

    if "point_time" not in df.columns or "value" not in df.columns:
        raise ValueError(
            f"Unexpected forecast columns returned: {list(df.columns)}"
        )

    df = df.rename(
        columns={
            "point_time": "timestamp",
            "value": "carbon_g_per_kwh"
        }
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df[["timestamp", "carbon_g_per_kwh"]].copy()
    df = df.sort_values("timestamp").reset_index(drop=True)

    return df

def get_watttime_forecast(region="CAISO_NORTH", signal_type="co2_moer"):
    """
    Fetch live WattTime forecast data and return it in Util's
    standardized dataframe format.

    Returned columns:
    - timestamp
    - carbon_g_per_kwh
    """
    forecast_json = get_forecast(region=region, signal_type=signal_type)
    return forecast_to_dataframe(forecast_json)