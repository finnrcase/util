from __future__ import annotations

"""
Streamlit-side client for the AI interpretation endpoint.

Calls POST /api/v1/ai/interpret on the Util FastAPI backend.
No AI provider logic lives here — all model calls are handled by the backend.
"""

import logging
import os
import time
from typing import Any

import pandas as pd
import requests

from src.runtime_config import get_project_root


_logger = logging.getLogger(__name__)

_DEFAULT_API_BASE = "http://127.0.0.1:8000"
_TIMEOUT_SECONDS = 45

_UNAVAILABLE: dict[str, Any] = {
    "status": "unavailable",
    "why_this_schedule": None,
    "tradeoff_summary": None,
    "scenario_comparison": None,
    "recommendation_memo": None,
    "message": "AI summary unavailable for this run.",
}


def _resolve_api_base() -> tuple[str, dict[str, Any]]:
    """
    Resolve the backend base URL.

    Resolution order:
      1. st.secrets["UTIL_API_BASE_URL"]
      2. os.getenv("UTIL_API_BASE_URL")
      3. http://127.0.0.1:8000  (local dev fallback only)

    Returns (resolved_url, {"resolved_url": ..., "url_source": ...}).
    """
    # 1. Streamlit secrets
    try:
        import streamlit as _st
        val = str(_st.secrets["UTIL_API_BASE_URL"]).strip().rstrip("/")
        if val:
            return val, {"resolved_url": val, "url_source": "streamlit_secrets"}
    except Exception:
        pass

    # 2. Environment variable / .env loaded into os.environ at startup
    val = os.getenv("UTIL_API_BASE_URL", "").strip().rstrip("/")
    if val:
        return val, {"resolved_url": val, "url_source": "environment_variable"}

    # 3. Hardcoded local-dev default
    _logger.warning(
        "Streamlit AI client: UTIL_API_BASE_URL not found in st.secrets or os.environ — "
        "falling back to %s (local dev only)",
        _DEFAULT_API_BASE,
    )
    return _DEFAULT_API_BASE, {"resolved_url": _DEFAULT_API_BASE, "url_source": "hardcoded_default"}


def _build_run_key(result: dict) -> str:
    """
    Stable dedup string for a completed optimization result.
    Changes when a new run produces different outputs. Used by Streamlit
    session state to avoid re-calling the endpoint on rerenders.
    """
    metrics = result.get("metrics", {})
    workload = result.get("workload_input")
    return "|".join([
        str(result.get("region", "")),
        str(getattr(workload, "objective", "")),
        str(metrics.get("optimized_cost", "")),
        str(metrics.get("optimized_carbon_kg", "")),
    ])


def _format_window(schedule_df: pd.DataFrame) -> str | None:
    """Format run window start→end from the schedule DataFrame."""
    try:
        run_rows = schedule_df[schedule_df["run_flag"] == 1]
        if run_rows.empty:
            return None
        timestamps = pd.to_datetime(run_rows["timestamp"]).sort_values()
        fmt = "%b %d, %I:%M %p"
        return f"{timestamps.iloc[0].strftime(fmt)} to {timestamps.iloc[-1].strftime(fmt)}"
    except Exception:
        return None


def build_ai_payload(result: dict) -> dict[str, Any]:
    """
    Build the AiInterpretRequest payload from a pipeline result dict.

    Reads only from already-computed optimizer output — no new calculations.
    """
    metrics = result.get("metrics", {})
    workload = result.get("workload_input")
    schedule_df = result.get("schedule")

    objective = getattr(workload, "objective", "unknown")
    deadline = getattr(workload, "deadline", None)
    region = result.get("region")
    window_summary = _format_window(schedule_df) if schedule_df is not None else None

    selected_result: dict[str, Any] = {
        "objective": objective,
        "projected_cost": (
            float(metrics["optimized_cost"])
            if metrics.get("optimized_cost") is not None else None
        ),
        "projected_emissions": (
            float(metrics["optimized_carbon_kg"])
            if metrics.get("optimized_carbon_kg") is not None else None
        ),
        "schedule_summary": window_summary or "Window unavailable",
    }

    alternatives: list[dict[str, Any]] = []
    baseline_cost = metrics.get("baseline_cost")
    baseline_carbon = metrics.get("baseline_carbon_kg")
    if baseline_cost is not None or baseline_carbon is not None:
        alternatives.append({
            "objective": "baseline",
            "projected_cost": float(baseline_cost) if baseline_cost is not None else None,
            "projected_emissions": float(baseline_carbon) if baseline_carbon is not None else None,
            "schedule_summary": "Run immediately without optimization",
        })

    return {
        "selected_objective": objective,
        "deadline": deadline.isoformat() if deadline is not None else None,
        "region": region,
        "selected_result": selected_result,
        "alternatives": alternatives,
    }


def call_interpret(result: dict) -> dict[str, Any]:
    """
    POST the optimizer result to /api/v1/ai/interpret and return the parsed response.

    Returns the unavailable fallback dict on any failure — never raises.
    Every return value includes a '_debug' key for the Streamlit debug expander.
    """
    api_base, url_diag = _resolve_api_base()
    url = f"{api_base}/api/v1/ai/interpret"

    debug: dict[str, Any] = {
        "resolved_url": url_diag["resolved_url"],
        "url_source": url_diag["url_source"],
        "endpoint": url,
        "outcome": None,
        "status_code": None,
        "elapsed_seconds": None,
        "started_at": None,
        "finished_at": None,
        "error_type": None,
        "error_detail": None,
        "response_json": None,
    }

    try:
        payload = build_ai_payload(result)

        _logger.info(
            "Streamlit AI client: POST %s objective=%s region=%s timeout=%ss",
            url,
            payload.get("selected_objective"),
            payload.get("region"),
            _TIMEOUT_SECONDS,
        )

        t_start = time.time()
        debug["started_at"] = time.strftime("%H:%M:%S", time.localtime(t_start))

        response = requests.post(url, json=payload, timeout=_TIMEOUT_SECONDS)

        t_end = time.time()
        debug["finished_at"] = time.strftime("%H:%M:%S", time.localtime(t_end))
        debug["elapsed_seconds"] = round(t_end - t_start, 2)
        debug["status_code"] = response.status_code

        response.raise_for_status()

        body = response.json()
        debug["outcome"] = "success"
        debug["response_json"] = body
        return {**body, "_debug": debug}

    except requests.ConnectionError as exc:
        t_end = time.time()
        debug["finished_at"] = time.strftime("%H:%M:%S", time.localtime(t_end))
        debug["elapsed_seconds"] = round(t_end - (time.time() - 0), 2)
        debug["outcome"] = "connection_error"
        debug["error_type"] = "ConnectionError"
        debug["error_detail"] = str(exc)
        _logger.info("Streamlit AI client: connection error to %s — %s", api_base, exc)
        return {**_UNAVAILABLE, "_debug": debug}

    except requests.Timeout:
        t_end = time.time()
        debug["finished_at"] = time.strftime("%H:%M:%S", time.localtime(t_end))
        debug["elapsed_seconds"] = _TIMEOUT_SECONDS
        debug["outcome"] = "timeout"
        debug["error_type"] = "Timeout"
        debug["error_detail"] = f"No response within {_TIMEOUT_SECONDS}s"
        _logger.warning("Streamlit AI client: request timed out after %ss", _TIMEOUT_SECONDS)
        return {**_UNAVAILABLE, "_debug": debug}

    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "unknown"
        body_text = exc.response.text if exc.response is not None else ""
        debug["outcome"] = "http_error"
        debug["error_type"] = "HTTPError"
        debug["error_detail"] = f"HTTP {status_code}: {body_text[:500]}"
        debug["status_code"] = status_code
        _logger.error("Streamlit AI client: HTTP error status=%s body=%s", status_code, body_text[:200])
        return {**_UNAVAILABLE, "_debug": debug}

    except Exception as exc:
        debug["outcome"] = "unexpected_error"
        debug["error_type"] = type(exc).__name__
        debug["error_detail"] = str(exc)
        _logger.error("Streamlit AI client: unexpected error type=%s detail=%s", type(exc).__name__, str(exc))
        return {**_UNAVAILABLE, "_debug": debug}
