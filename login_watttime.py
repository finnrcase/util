from pathlib import Path
import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
ENV_PATH = PROJECT_ROOT / ".env"

print("Looking for .env at:", ENV_PATH)
print(".env exists:", ENV_PATH.exists())

load_dotenv(dotenv_path=ENV_PATH)

username = os.getenv("WATTTIME_USERNAME")
password = os.getenv("WATTTIME_PASSWORD")

print("Loaded username:", username)
print("Password loaded:", password is not None)

if not username or not password:
    raise ValueError("Missing WATTTIME_USERNAME or WATTTIME_PASSWORD in .env")

login_url = "https://api2.watttime.org/v2/login"

response = requests.get(
    login_url,
    auth=HTTPBasicAuth(username, password),
    timeout=30
)

print("Status code:", response.status_code)
print("Content-Type:", response.headers.get("content-type"))
print("Response text:", response.text)