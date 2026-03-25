from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None

try:
    import streamlit as st
except ModuleNotFoundError:
    st = None


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
def load_project_env() -> Path:
    project_root = get_project_root()
    env_path = project_root / ".env"

    if load_dotenv is not None:
        load_dotenv(env_path, override=True)

    return env_path

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
    return {
        "app_mode": get_app_mode(),
        "python_env": str(get_setting("PYTHON_ENV", "")) or "unset",
        "analytics_logging_enabled": get_bool_setting("UTIL_ANALYTICS_ENABLED", True),
        "watttime_configured": bool(get_setting("WATTTIME_USERNAME")) and bool(get_setting("WATTTIME_PASSWORD")),
        "carbon_price_configured": get_setting("UTIL_CARBON_PRICE_USD_PER_TON") is not None,
        "clean_energy_credit_configured": get_setting("UTIL_CLEAN_ENERGY_CREDIT_USD") is not None,
        "electricity_adder_configured": get_setting("UTIL_ELECTRICITY_PRICE_ADDER_PCT") is not None,
        "streamlit_secrets_available": bool(_streamlit_secrets_dict()),
        "show_runtime_diagnostics": get_bool_setting("UTIL_SHOW_RUNTIME_DIAGNOSTICS", False),
    }
