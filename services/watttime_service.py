import os
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

USERNAME = os.getenv("WATTTIME_USERNAME")
PASSWORD = os.getenv("WATTTIME_PASSWORD")

LOGIN_URL = "https://api2.watttime.org/v2/login"
FORECAST_URL = "https://api.watttime.org/v3/forecast"


def get_token():
    """
    Log in to WattTime and return a bearer token.
    """
    if not USERNAME or not PASSWORD:
        raise ValueError(
            "WattTime credentials are missing. "
            "Set WATTTIME_USERNAME and WATTTIME_PASSWORD in the environment."
        )

    response = requests.get(
        LOGIN_URL,
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        timeout=30
    )

    if response.status_code == 401:
        raise ValueError("WattTime authentication failed: unauthorized (401). Check credentials.")

    if response.status_code == 403:
        raise ValueError("WattTime authentication failed: forbidden (403). Check credentials and account access.")

    response.raise_for_status()

    token = response.json().get("token")
    if not token:
        raise ValueError("WattTime login succeeded but no token was returned.")

    return token


def get_forecast(region="CAISO_NORTH", signal_type="co2_moer"):
    """
    Fetch live carbon forecast data from WattTime.
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

    if response.status_code == 401:
        raise ValueError("WattTime forecast request failed: unauthorized (401).")

    if response.status_code == 403:
        raise ValueError("WattTime forecast request failed: forbidden (403).")

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