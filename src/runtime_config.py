from __future__ import annotations

import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

try:
    from dotenv import dotenv_values, load_dotenv
except ModuleNotFoundError:
    dotenv_values = None
    load_dotenv = None

try:
    import streamlit as st
except ModuleNotFoundError:
    st = None

AWS_CLOUD_SETTING_NAMES = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "S3_BUCKET_NAME",
)


def _candidate_project_roots() -> list[Path]:
    candidates: list[Path] = []
    module_path = Path(__file__).resolve()
    cwd_path = Path.cwd().resolve()

    for base in [module_path.parent, *module_path.parents, cwd_path, *cwd_path.parents]:
        if base not in candidates:
            candidates.append(base)

    return candidates


@lru_cache(maxsize=1)
def get_project_root() -> Path:
    for candidate in _candidate_project_roots():
        if (candidate / "app.py").exists() and (candidate / "src").is_dir():
            return candidate

    return Path(__file__).resolve().parents[1]


@lru_cache(maxsize=1)
def get_app_storage_root(app_name: str = "Util") -> Path:
    override = str(get_setting("UTIL_APP_DATA_DIR", "") or "").strip()
    if override:
        return Path(override).expanduser().resolve()

    home = Path.home()
    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA") or (home / "AppData" / "Local"))
    elif sys.platform == "darwin":
        base = home / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_DATA_HOME") or (home / ".local" / "share"))

    return (base / app_name).resolve()

@lru_cache(maxsize=1)
def load_project_env() -> Path:
    project_root = get_project_root()
    env_path = project_root / ".env"

    if load_dotenv is not None:
        load_dotenv(env_path, override=True)

    return env_path


@lru_cache(maxsize=1)
def get_project_env_diagnostics() -> dict[str, Any]:
    env_path = load_project_env()
    exists = env_path.exists()
    raw_text = env_path.read_text(encoding="utf-8") if exists else ""
    is_empty = not raw_text.strip()

    parsed_values: dict[str, Any] = {}
    if dotenv_values is not None and exists:
        parsed_values = dict(dotenv_values(env_path))

    parsed_keys = [key for key in parsed_values.keys() if key]
    return {
        "env_path": str(env_path),
        "exists": exists,
        "is_empty": is_empty,
        "parsed_keys": parsed_keys,
        "has_aws_access_key_id": "AWS_ACCESS_KEY_ID" in parsed_values,
        "has_aws_secret_access_key": "AWS_SECRET_ACCESS_KEY" in parsed_values,
        "has_aws_region": "AWS_REGION" in parsed_values,
        "has_s3_bucket_name": "S3_BUCKET_NAME" in parsed_values,
    }

if load_dotenv is not None:
    load_project_env()


@lru_cache(maxsize=1)
def _streamlit_secrets_dict() -> dict[str, Any]:
    if st is None:
        return {}

    try:
        return dict(st.secrets)
    except Exception:
        return {}


def _clean_setting_value(value: Any) -> str:
    return str(value or "").strip()


def get_env_setting(name: str, default: Any = None) -> Any:
    if name in os.environ:
        return os.environ[name]
    return default


@lru_cache(maxsize=1)
def resolve_cloud_config() -> dict[str, Any]:
    secrets = _streamlit_secrets_dict()
    secret_values = {name: _clean_setting_value(secrets.get(name, "")) for name in AWS_CLOUD_SETTING_NAMES}
    env_values = {name: _clean_setting_value(get_env_setting(name, "")) for name in AWS_CLOUD_SETTING_NAMES}

    secrets_complete = all(secret_values.values())
    env_complete = all(env_values.values())

    if secrets_complete:
        return {
            "source": "streamlit secrets",
            "values": secret_values,
            "configured": True,
        }

    if env_complete:
        return {
            "source": "environment/.env",
            "values": env_values,
            "configured": True,
        }

    return {
        "source": "missing",
        "values": env_values,
        "configured": False,
    }


def get_setting(name: str, default: Any = None) -> Any:
    if name in os.environ:
        return os.environ[name]

    secrets = _streamlit_secrets_dict()
    if name in secrets:
        return secrets[name]

    return default


def get_bool_setting(name: str, default: bool = False) -> bool:
    value = get_setting(name, default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def get_float_setting(name: str, default: float | None = None) -> float | None:
    value = get_setting(name, default)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def get_app_mode() -> str:
    return str(get_setting("APP_MODE", get_setting("DEV_MODE", "live"))).strip() or "live"


def get_runtime_diagnostics() -> dict[str, Any]:
    cloud_config = resolve_cloud_config()
    return {
        "app_mode": get_app_mode(),
        "python_env": str(get_setting("PYTHON_ENV", "")) or "unset",
        "analytics_logging_enabled": get_bool_setting("UTIL_ANALYTICS_ENABLED", True),
        "watttime_configured": bool(get_setting("WATTTIME_USERNAME")) and bool(get_setting("WATTTIME_PASSWORD")),
        "carbon_price_configured": get_setting("UTIL_CARBON_PRICE_USD_PER_TON") is not None,
        "clean_energy_credit_configured": get_setting("UTIL_CLEAN_ENERGY_CREDIT_USD") is not None,
        "electricity_adder_configured": get_setting("UTIL_ELECTRICITY_PRICE_ADDER_PCT") is not None,
        "streamlit_secrets_available": bool(_streamlit_secrets_dict()),
        "cloud_config_source": cloud_config["source"],
        "cloud_configured": cloud_config["configured"],
        "show_runtime_diagnostics": get_bool_setting("UTIL_SHOW_RUNTIME_DIAGNOSTICS", False),
    }
