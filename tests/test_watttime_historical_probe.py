import os
import json
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

# CHANGE THIS if you already know a different historical endpoint path
HISTORICAL_URL = "https://api2.watttime.org/v2/historical"


def get_token() -> str:
    if not USERNAME or not PASSWORD:
        raise ValueError(
            "Missing WATTTIME_USERNAME or WATTTIME_PASSWORD in environment."
        )

    response = requests.get(
        LOGIN_URL,
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        timeout=30,
    )

    print("\n=== LOGIN ===")
    print("STATUS:", response.status_code)
    print("URL:", response.url)
    print("CONTENT-TYPE:", response.headers.get("content-type"))
    print("RESPONSE PREVIEW:", response.text[:300])

    response.raise_for_status()

    token = response.json().get("token")
    if not token:
        raise ValueError("Login succeeded but no token was returned.")

    return token


def probe_historical():
    token = get_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    # Last 3 days, UTC
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=3)

    params = {
        "region": "CAISO_NORTH",
        "signal_type": "co2_moer",
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
    }

    response = requests.get(
        HISTORICAL_URL,
        headers=headers,
        params=params,
        timeout=60,
    )

    print("\n=== HISTORICAL REQUEST ===")
    print("STATUS:", response.status_code)
    print("URL:", response.url)
    print("CONTENT-TYPE:", response.headers.get("content-type"))
    print("TEXT PREVIEW:", response.text[:1000])

    try:
        response.raise_for_status()
    except Exception as e:
        print("\nREQUEST FAILED.")
        print(type(e).__name__, str(e))
        return

    content_type = response.headers.get("content-type", "")
    if "application/json" not in content_type:
        print("\nResponse is not JSON.")
        return

    try:
        payload = response.json()
    except Exception as e:
        print("\nCould not parse JSON.")
        print(type(e).__name__, str(e))
        return

    print("\n=== JSON STRUCTURE ===")
    print("TYPE:", type(payload))

    if isinstance(payload, dict):
        print("TOP-LEVEL KEYS:", list(payload.keys()))

        for key, value in payload.items():
            print(f"\nKEY: {key}")
            print("VALUE TYPE:", type(value))

            if isinstance(value, list):
                print("LIST LENGTH:", len(value))
                print("FIRST 2 ITEMS:")
                print(json.dumps(value[:2], indent=2, default=str)[:2000])

            elif isinstance(value, dict):
                print("DICT KEYS:", list(value.keys()))
                print(json.dumps(value, indent=2, default=str)[:2000])

            else:
                print("VALUE:", str(value)[:500])

    elif isinstance(payload, list):
        print("LIST LENGTH:", len(payload))
        print("FIRST 3 ITEMS:")
        print(json.dumps(payload[:3], indent=2, default=str)[:3000])
    else:
        print("RAW PAYLOAD:")
        print(str(payload)[:2000])

    # Try turning likely row containers into a dataframe for inspection
    candidate_rows = None

    if isinstance(payload, list):
        candidate_rows = payload
    elif isinstance(payload, dict):
        for key in ["data", "results", "historical", "values"]:
            if key in payload and isinstance(payload[key], list):
                candidate_rows = payload[key]
                print(f"\nUsing payload['{key}'] as candidate rows.")
                break

    if candidate_rows is not None and len(candidate_rows) > 0:
        try:
            df = pd.DataFrame(candidate_rows)
            print("\n=== DATAFRAME PREVIEW ===")
            print("COLUMNS:", list(df.columns))
            print(df.head(5).to_string())
        except Exception as e:
            print("\nCould not convert candidate rows to DataFrame.")
            print(type(e).__name__, str(e))
    else:
        print("\nNo obvious list of row records found in payload.")


if __name__ == "__main__":
    probe_historical()