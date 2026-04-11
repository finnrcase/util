from __future__ import annotations

"""
Streamlit-side client for the AI interpretation endpoint.

Calls POST /api/v1/ai/interpret on the Util FastAPI backend.
No AI provider logic lives here — all model calls are handled by the backend.
"""

import logging
from typing import Any

import pandas as pd
import requests

from src.runtime_config import get_setting


_logger = logging.getLogger(__name__)

_DEFAULT_API_BASE = "http://127.0.0.1:8000"
_TIMEOUT_SECONDS = 15

_UNAVAILABLE: dict[str, Any] = {
    "status": "unavailable",
    "why_this_schedule": None,
    "tradeoff_summary": None,
    "scenario_comparison": None,
    "recommendation_memo": None,
    "message": "AI summary unavailable for this run.",
}


def _get_api_base() -> str:
    return str(get_setting("UTIL_API_BASE_URL", _DEFAULT_API_BASE)).rstrip("/")


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
    Matches the payload shape used by the React frontend.
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

    # Include the baseline as an alternative so the AI has comparison data.
    # Both values come directly from the pipeline result — no recomputation.
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
    The backend handles all AI provider logic; this function is HTTP only.
    """
    try:
        payload = build_ai_payload(result)
        url = f"{_get_api_base()}/api/v1/ai/interpret"
        _logger.info(
            "Streamlit AI client: POST %s objective=%s region=%s",
            url,
            payload.get("selected_objective"),
            payload.get("region"),
        )
        response = requests.post(url, json=payload, timeout=_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
    except requests.ConnectionError:
        _logger.info(
            "Streamlit AI client: backend not reachable at %s — "
            "set UTIL_API_BASE_URL if the backend runs on a different address",
            _get_api_base(),
        )
        return _UNAVAILABLE
    except requests.Timeout:
        _logger.warning(
            "Streamlit AI client: request timed out after %ss", _TIMEOUT_SECONDS
        )
        return _UNAVAILABLE
    except requests.HTTPError as exc:
        _logger.error(
            "Streamlit AI client: HTTP error status=%s",
            exc.response.status_code if exc.response else "unknown",
        )
        return _UNAVAILABLE
    except Exception as exc:
        _logger.error(
            "Streamlit AI client: unexpected error type=%s", type(exc).__name__
        )
        return _UNAVAILABLE
