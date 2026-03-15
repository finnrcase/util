import os
from datetime import datetime, timedelta, timezone

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

CANDIDATE_URLS = [
    "https://api2.watttime.org/v2/historical",
    "https://api2.watttime.org/v3/historical",
    "https://api.watttime.org/v2/historical",
    "https://api.watttime.org/v3/historical",
    "https://api2.watttime.org/v2/data",
    "https://api2.watttime.org/v3/data",
    "https://api.watttime.org/v2/data",
    "https://api.watttime.org/v3/data",
]

def get_token() -> str:
    if not USERNAME or not PASSWORD:
        raise ValueError("Missing WATTTIME_USERNAME or WATTTIME_PASSWORD.")

    response = requests.get(
        LOGIN_URL,
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        timeout=30,
    )
    response.raise_for_status()

    token = response.json().get("token")
    if not token:
        raise ValueError("Login succeeded but no token was returned.")
    return token


def main():
    token = get_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=3)

    params = {
        "region": "CAISO_NORTH",
        "signal_type": "co2_moer",
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
    }

    for url in CANDIDATE_URLS:
        print("\n" + "=" * 80)
        print("TESTING:", url)

        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=30,
                allow_redirects=False,
            )

            print("STATUS:", response.status_code)
            print("FINAL URL:", response.url)
            print("CONTENT-TYPE:", response.headers.get("content-type"))
            print("LOCATION HEADER:", response.headers.get("location"))
            print("TEXT PREVIEW:", response.text[:400])

        except Exception as e:
            print("ERROR:", type(e).__name__, str(e))


if __name__ == "__main__":
    main()