from __future__ import annotations

import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


LOGIN_URL = "https://api2.watttime.org/v2/login"
LOCATION_URL = "https://api.watttime.org/v3/region-from-loc"
LATITUDE = 34.05
LONGITUDE = -118.25
SIGNAL_TYPE = "co2_moer"


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")

    username = os.getenv("WATTTIME_USERNAME")
    password = os.getenv("WATTTIME_PASSWORD")

    if not username or not password:
        raise RuntimeError("Missing WATTTIME_USERNAME or WATTTIME_PASSWORD in environment.")

    print("WATTTIME LOCATION ACCESS DIAGNOSTIC")
    print(f"LOGIN URL: {LOGIN_URL}")
    print(f"LOCATION URL: {LOCATION_URL}")
    print(f"LOCATION PARAMS: latitude={LATITUDE}, longitude={LONGITUDE}, signal_type={SIGNAL_TYPE}")
    print("-" * 80)

    login_response = requests.get(LOGIN_URL, auth=(username, password), timeout=30)
    print("LOGIN STATUS CODE:", login_response.status_code)
    print("LOGIN RESPONSE BODY:")
    print(login_response.text)
    print("-" * 80)

    login_json = login_response.json()
    token = login_json.get("token")
    if not token:
        raise RuntimeError("Login succeeded without returning a token.")

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "signal_type": SIGNAL_TYPE,
    }
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(LOCATION_URL, params=params, headers=headers, timeout=30)

    print("LOCATION REQUEST URL:", response.request.url)
    print("LOCATION STATUS CODE:", response.status_code)
    print("RESPONSE BODY:")
    print(response.text)
    print("-" * 80)

    if response.status_code == 403:
        print("LOCATION ACCESS: NOT ENABLED")
        print("INTERPRETATION: this is a permission issue. The account authenticated successfully but is not allowed to use /v3/region-from-loc.")
        return

    if response.status_code != 200:
        print("LOCATION ACCESS: INCONCLUSIVE")
        print(f"INTERPRETATION: the request did not succeed and it was not a 403 permission denial. HTTP {response.status_code} does not conclusively prove whether location access is enabled.")
        response.raise_for_status()

    response_json = response.json()
    region = response_json.get("region") or response_json.get("abbrev") or response_json.get("name")
    if region:
        print("LOCATION ACCESS: ENABLED")
        print("REGION RETURNED:")
        print(json.dumps(response_json, indent=2))
        print(f"REGION SUMMARY: {region}")
        print("INTERPRETATION: the endpoint returned a region successfully, so location-based access is enabled.")
        return

    print("LOCATION ACCESS: INCONCLUSIVE")
    print("RESPONSE JSON:")
    print(json.dumps(response_json, indent=2))
    print("INTERPRETATION: the request returned HTTP 200 but did not include an obvious region field, so the result is inconclusive.")


if __name__ == "__main__":
    main()
