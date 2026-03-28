"""
WattTime service helpers for Util.

This module handles:
- authentication
- generic authenticated JSON requests
- live forecast fetches
- historical fetches
- location -> region lookup
- conversion of WattTime JSON into Util dataframe format
"""

from __future__ import annotations

from functools import lru_cache
from datetime import datetime, timedelta, timezone
import logging
from typing import Any

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from src.runtime_config import get_setting

LOGIN_URL = "https://api2.watttime.org/v2/login"
FORECAST_URL = "https://api.watttime.org/v3/forecast"
HISTORICAL_URL = "https://api.watttime.org/v3/historical"
REGION_FROM_LOC_URL = "https://api.watttime.org/v3/region-from-loc"
WATTTIME_LOGIN_TIMEOUT_SECONDS = 20
WATTTIME_REQUEST_TIMEOUT_SECONDS = 25
watttime_logger = logging.getLogger("uvicorn.error")


@lru_cache(maxsize=1)
def get_token() -> str:
    """
    Log in to WattTime and return a bearer token.
    """
    username = get_setting("WATTTIME_USERNAME")
    password = get_setting("WATTTIME_PASSWORD")

    if not username or not password:
        raise ValueError(
            "WattTime credentials are missing. "
            "Set WATTTIME_USERNAME and WATTTIME_PASSWORD in the environment."
        )

    watttime_logger.info("Util WattTime: token fetch start login_url=%s", LOGIN_URL)
    response = requests.get(
        LOGIN_URL,
        auth=HTTPBasicAuth(str(username), str(password)),
        timeout=WATTTIME_LOGIN_TIMEOUT_SECONDS,
    )
    watttime_logger.info("Util WattTime: token fetch response status=%s", response.status_code)

    if response.status_code == 401:
        raise ValueError(
            "WattTime authentication failed: unauthorized (401). Check credentials."
        )

    if response.status_code == 403:
        raise ValueError(
            "WattTime authentication failed: forbidden (403). "
            "Check credentials and account access."
        )

    response.raise_for_status()

    token = response.json().get("token")
    if not token:
        raise ValueError("WattTime login succeeded but no token was returned.")

    watttime_logger.info("Util WattTime: token fetch success")
    return token


def _fetch_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    """
    Make an authenticated GET request to a WattTime endpoint and return JSON.
    """
    token = get_token()
    response = None

    for attempt in range(2):
        headers = {
            "Authorization": f"Bearer {token}",
        }

        watttime_logger.info(
            "Util WattTime: request start url=%s attempt=%s params=%s",
            url,
            attempt + 1,
            params,
        )
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=WATTTIME_REQUEST_TIMEOUT_SECONDS,
        )
        watttime_logger.info(
            "Util WattTime: request response status=%s url=%s",
            response.status_code,
            response.url,
        )

        if response.status_code != 401 or attempt == 1:
            break

        watttime_logger.warning("Util WattTime: received 401, clearing cached token and retrying once")
        get_token.cache_clear()
        token = get_token()

    if response is None:
        raise ValueError("WattTime request did not return a response.")

    if response.status_code == 401:
        raise ValueError("WattTime request failed: unauthorized (401).")

    if response.status_code == 403:
        raise ValueError("WattTime request failed: forbidden (403).")

    response.raise_for_status()

    content_type = response.headers.get("content-type", "")
    if "application/json" not in content_type:
        raise ValueError(
            f"WattTime endpoint did not return JSON. "
            f"URL={response.url} | content-type={content_type}"
        )

    return response.json()


def forecast_to_dataframe(
    payload: dict[str, Any] | list[dict[str, Any]]
) -> pd.DataFrame:
    """
    Convert WattTime v3 forecast/historical JSON into Util's standard dataframe format.
    """
    if isinstance(payload, dict):
        if "data" not in payload:
            raise ValueError(
                f"Expected payload to contain a 'data' key. "
                f"Keys returned: {list(payload.keys())}"
            )
        rows = payload["data"]

    elif isinstance(payload, list):
        rows = payload

    else:
        raise ValueError(f"Unexpected payload type: {type(payload)}")

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("WattTime response was empty.")

    required_columns = {"point_time", "value"}
    if not required_columns.issubset(df.columns):
        raise ValueError(
            f"Unexpected WattTime columns returned: {list(df.columns)}"
        )

    df = df.rename(
        columns={
            "point_time": "timestamp",
            "value": "carbon_g_per_kwh",
        }
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df[["timestamp", "carbon_g_per_kwh"]].copy()
    df = df.sort_values("timestamp").reset_index(drop=True)

    return df


def get_forecast(
    region: str = "CAISO_NORTH",
    signal_type: str = "co2_moer",
) -> dict[str, Any]:
    """
    Fetch live carbon forecast JSON from WattTime.
    """
    params = {
        "region": region,
        "signal_type": signal_type,
    }
    return _fetch_json(FORECAST_URL, params)


def get_historical(
    region: str = "CAISO_NORTH",
    signal_type: str = "co2_moer",
    start: str | None = None,
    end: str | None = None,
    days: int = 7,
) -> dict[str, Any]:
    """
    Fetch historical carbon JSON from WattTime v3 historical endpoint.
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


def get_region_from_loc(
    latitude: float,
    longitude: float,
    signal_type: str = "co2_moer",
) -> dict[str, Any]:
    """
    Resolve latitude/longitude to a WattTime region.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "signal_type": signal_type,
    }
    return _fetch_json(REGION_FROM_LOC_URL, params)


def get_ba_from_loc(
    latitude: float,
    longitude: float,
    signal_type: str = "co2_moer",
) -> dict[str, Any]:
    """
    Backward-compatible alias for older code that still imports get_ba_from_loc().
    """
    return get_region_from_loc(
        latitude=latitude,
        longitude=longitude,
        signal_type=signal_type,
    )


@lru_cache(maxsize=16)
def _get_watttime_forecast_cached(
    region: str,
    signal_type: str,
) -> tuple[tuple[pd.Timestamp, float], ...]:
    forecast_json = get_forecast(region=region, signal_type=signal_type)
    forecast_df = forecast_to_dataframe(forecast_json)
    return tuple(
        (row.timestamp, float(row.carbon_g_per_kwh))
        for row in forecast_df.itertuples(index=False)
    )


def get_watttime_forecast(
    region: str = "CAISO_NORTH",
    signal_type: str = "co2_moer",
) -> pd.DataFrame:
    """
    Fetch live WattTime forecast data and return it in Util's standardized dataframe format.
    """
    cached_rows = _get_watttime_forecast_cached(region, signal_type)
    return pd.DataFrame(cached_rows, columns=["timestamp", "carbon_g_per_kwh"])


@lru_cache(maxsize=16)
def _get_watttime_historical_cached(
    region: str,
    signal_type: str,
    start: str | None,
    end: str | None,
    days: int,
) -> tuple[tuple[pd.Timestamp, float], ...]:
    historical_json = get_historical(
        region=region,
        signal_type=signal_type,
        start=start,
        end=end,
        days=days,
    )
    historical_df = forecast_to_dataframe(historical_json)
    return tuple(
        (row.timestamp, float(row.carbon_g_per_kwh))
        for row in historical_df.itertuples(index=False)
    )


def get_watttime_historical(
    region: str = "CAISO_NORTH",
    signal_type: str = "co2_moer",
    start: str | None = None,
    end: str | None = None,
    days: int = 7,
) -> pd.DataFrame:
    """
    Fetch historical WattTime data and return it in Util's standardized dataframe format.
    """
    cached_rows = _get_watttime_historical_cached(region, signal_type, start, end, days)
    return pd.DataFrame(cached_rows, columns=["timestamp", "carbon_g_per_kwh"])
