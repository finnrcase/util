import os
from datetime import datetime, timedelta, timezone

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
HISTORICAL_URL = "https://api.watttime.org/v3/historical"


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


def _fetch_json(url: str, params: dict) -> dict:
    token = get_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=60
    )

    print("REQUEST STATUS:", response.status_code)
    print("REQUEST CONTENT TYPE:", response.headers.get("content-type"))
    print("REQUEST URL:", response.url)
    print("REQUEST RESPONSE PREVIEW:", response.text[:500])

    if response.status_code == 401:
        raise ValueError("WattTime request failed: unauthorized (401).")

    if response.status_code == 403:
        raise ValueError("WattTime request failed: forbidden (403).")

    response.raise_for_status()

    if "application/json" not in response.headers.get("content-type", ""):
        raise ValueError("WattTime endpoint did not return JSON.")

    return response.json()


def forecast_to_dataframe(payload: dict | list) -> pd.DataFrame:
    """
    Convert WattTime v3 forecast/historical JSON into Util's standard dataframe format.

    Expected normalized columns:
    - timestamp
    - carbon_g_per_kwh
    """
    if isinstance(payload, dict):
        if "data" not in payload:
            raise ValueError(
                f"Expected payload to contain a 'data' key. Keys returned: {list(payload.keys())}"
            )
        rows = payload["data"]
    elif isinstance(payload, list):
        rows = payload
    else:
        raise ValueError(f"Unexpected payload type: {type(payload)}")

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("WattTime response was empty.")

    if "point_time" not in df.columns or "value" not in df.columns:
        raise ValueError(
            f"Unexpected WattTime columns returned: {list(df.columns)}"
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


def get_forecast(region="CAISO_NORTH", signal_type="co2_moer"):
    """
    Fetch live carbon forecast data from WattTime.
    """
    params = {
        "region": region,
        "signal_type": signal_type,
    }
    return _fetch_json(FORECAST_URL, params)


def get_historical(
    region="CAISO_NORTH",
    signal_type="co2_moer",
    start: str | None = None,
    end: str | None = None,
    days: int = 7,
):
    """
    Fetch historical carbon data from WattTime v3 historical endpoint.
    If start/end are not provided, fetch the last `days` days in UTC.
    """
    if start is None or end is None:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days)
        start = start_dt.isoformat()
        end = end_dt.isoformat()

    params = {
        "region": region,
        "signal_type": signal_type,
        "start": start,
        "end": end,
    }

    return _fetch_json(HISTORICAL_URL, params)


def get_watttime_forecast(region="CAISO_NORTH", signal_type="co2_moer"):
    """
    Fetch live WattTime forecast data and return it in Util's standardized dataframe format.
    """
    forecast_json = get_forecast(region=region, signal_type=signal_type)
    return forecast_to_dataframe(forecast_json)


def get_watttime_historical(
    region="CAISO_NORTH",
    signal_type="co2_moer",
    start: str | None = None,
    end: str | None = None,
    days: int = 7,
):
    """
    Fetch historical WattTime data and return it in Util's standardized dataframe format.
    """
    historical_json = get_historical(
        region=region,
        signal_type=signal_type,
        start=start,
        end=end,
        days=days,
    )
    return forecast_to_dataframe(historical_json)