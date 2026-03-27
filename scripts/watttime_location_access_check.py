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
    print(f"LOCATION PARAMS: latitude={LATITUDE}, longitude={LONGITUDE}")
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

    params = {"latitude": LATITUDE, "longitude": LONGITUDE}
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(LOCATION_URL, params=params, headers=headers, timeout=30)

    print("REQUEST URL:", response.request.url)
    print("STATUS CODE:", response.status_code)
    print("RESPONSE BODY:")
    print(response.text)
    print("-" * 80)

    if response.status_code == 403:
        print("LOCATION ACCESS: NOT ENABLED")
        print("403 DETECTED: permission issue")
        print("MEANING: the WattTime credentials authenticated, but this account is not allowed to use /v3/region-from-loc.")
        return

    if not response.ok:
        print("LOCATION ACCESS: NOT ENABLED")
        print(f"REQUEST FAILED: HTTP {response.status_code}")
        response.raise_for_status()

    response_json = response.json()
    print("LOCATION ACCESS: ENABLED")
    print("REGION RETURNED:")
    print(json.dumps(response_json, indent=2))
    region = response_json.get("region") or response_json.get("abbrev") or response_json.get("name")
    if region:
        print(f"REGION SUMMARY: {region}")
    print("CONFIRMATION: location-based access is enabled for /v3/region-from-loc.")


if __name__ == "__main__":
    main()
