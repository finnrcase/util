from __future__ import annotations

import collections
import logging
import time
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from src.runtime_config import get_setting
from src.services.ai.schemas import AiInterpretRequest, AiInterpretResponse


ai_router = APIRouter(prefix="/api/v1/ai", tags=["ai"])
ai_route_logger = logging.getLogger("uvicorn.error")

# ---------------------------------------------------------------------------
# In-process sliding-window rate limiter (per client IP).
# Scoped to this router only — does not affect any existing routes.
# ---------------------------------------------------------------------------

_RATE_WINDOW_SECONDS = 60
_RATE_LIMIT_DEFAULT = 10

# Maps client IP -> deque of monotonic request timestamps within the window.
_request_log: dict[str, collections.deque[float]] = {}


def _get_rate_limit() -> int:
    try:
        return int(get_setting("AI_SUMMARY_RATE_LIMIT", _RATE_LIMIT_DEFAULT))
    except (TypeError, ValueError):
        return _RATE_LIMIT_DEFAULT


def _check_rate_limit(client_ip: str) -> bool:
    """
    Returns True if the request is within the allowed rate.
    Evicts expired timestamps from the sliding window in-place.
    """
    now = time.monotonic()
    limit = _get_rate_limit()

    if client_ip not in _request_log:
        _request_log[client_ip] = collections.deque()

    window = _request_log[client_ip]
    while window and now - window[0] > _RATE_WINDOW_SECONDS:
        window.popleft()

    if len(window) >= limit:
        return False

    window.append(now)
    return True


# ---------------------------------------------------------------------------
# Safe fallback response — used on any unhandled exception.
# ---------------------------------------------------------------------------

def _unavailable_json(message: str = "AI summary unavailable for this run.") -> JSONResponse:
    return JSONResponse(
        status_code=200,
        content={
            "status": "unavailable",
            "message": message,
            "why_this_schedule": None,
            "tradeoff_summary": None,
            "scenario_comparison": None,
            "recommendation_memo": None,
        },
    )


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@ai_router.post("/interpret", response_model=AiInterpretResponse)
def interpret(request_body: AiInterpretRequest, request: Request) -> Any:
    """
    Accept a structured optimizer result and return an AI-generated
    plain-language interpretation.

    Uses AiInterpretRequest — decoupled from the optimizer input schema.
    Required fields: selected_objective, selected_result.
    Optional fields: deadline, region, alternatives.

    - Rate-limited per client IP.
    - Returns status="unavailable" if AI is disabled or unconfigured.
    - Never exposes provider secrets or raw stack traces.
    """
    client_ip = request.client.host if request.client else "unknown"

    if not _check_rate_limit(client_ip):
        ai_route_logger.warning(
            "Util AI route: rate limit exceeded client=%s", client_ip
        )
        return JSONResponse(
            status_code=429,
            content={
                "status": "unavailable",
                "message": "AI summary rate limit exceeded. Try again shortly.",
            },
        )

    ai_route_logger.info(
        "Util AI route: interpret start client=%s objective=%s region=%s alternatives=%s",
        client_ip,
        request_body.selected_objective,
        request_body.region or "unset",
        len(request_body.alternatives),
    )

    try:
        from src.services.ai.ai_service import interpret as run_interpret

        result = run_interpret(request_body)

        ai_route_logger.info(
            "Util AI route [ROUTE] complete: status=%s client=%s",
            result.status,
            client_ip,
        )
        return result

    except Exception as exc:
        # This block fires only if interpret() raises unexpectedly (it should not).
        # The [ROUTE-EXC] tag makes this easy to find in uvicorn logs.
        ai_route_logger.exception(
            "Util AI route [ROUTE-EXC] unhandled exception type=%s client=%s — "
            "this indicates a code bug, not a config problem",
            type(exc).__name__,
            client_ip,
        )
        return _unavailable_json()
