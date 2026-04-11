from __future__ import annotations

import json
import logging
from typing import Any

from src.runtime_config import get_bool_setting, get_setting
from src.services.ai.prompts import SYSTEM_PROMPT, build_interpret_prompt
from src.services.ai.schemas import AiInterpretRequest, AiInterpretResponse


ai_logger = logging.getLogger("uvicorn.error")

_DEFAULT_MODEL = "claude-haiku-4-5-20251001"
_MAX_TOKENS = 600

_UNAVAILABLE = AiInterpretResponse(
    status="unavailable",
    message="AI summary unavailable for this run.",
)

_ERROR = AiInterpretResponse(
    status="error",
    message="AI summary unavailable for this run.",
)


def _is_enabled() -> bool:
    return get_bool_setting("AI_SUMMARY_ENABLED", True)


def _get_api_key() -> str | None:
    value = get_setting("ANTHROPIC_API_KEY")
    return str(value).strip() if value else None


def _get_model() -> str:
    value = get_setting("AI_SUMMARY_MODEL", _DEFAULT_MODEL)
    return str(value).strip() or _DEFAULT_MODEL


def _parse_ai_json(text: str) -> dict[str, Any]:
    """
    Parse JSON from the model response.
    Strips markdown code fences if the model wraps its output in them.
    """
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        inner_lines = [
            line for line in lines[1:]
            if line.strip() != "```"
        ]
        stripped = "\n".join(inner_lines).strip()
    return json.loads(stripped)


def interpret(request: AiInterpretRequest) -> AiInterpretResponse:
    """
    Call the Anthropic API with a grounded prompt built from optimizer output.

    Returns AiInterpretResponse. Never raises — returns unavailable or error
    status on any failure. Provider API key is never surfaced to callers.
    """
    # Diagnostic entry log — always fires on every interpret call.
    # Shows the resolved values of every config gate so the exit reason is visible
    # in uvicorn logs without exposing secret values.
    key_present = bool(_get_api_key())
    enabled = _is_enabled()
    model = _get_model()
    ai_logger.info(
        "Util AI service [DIAG] entry: enabled=%s key_present=%s model=%s objective=%s",
        enabled,
        key_present,
        model,
        request.selected_objective,
    )

    # [AI-1] Enabled check
    if not enabled:
        ai_logger.info("Util AI service [AI-1] exit: AI_SUMMARY_ENABLED is false or unset")
        return _UNAVAILABLE

    # [AI-2] API key check
    api_key = _get_api_key()
    if not api_key:
        ai_logger.info("Util AI service [AI-2] exit: ANTHROPIC_API_KEY is missing or empty in env")
        return _UNAVAILABLE

    # [AI-3] Package availability check
    try:
        import anthropic
    except ImportError:
        ai_logger.warning(
            "Util AI service [AI-3] exit: 'anthropic' package not installed — "
            "run: pip install anthropic  (or: pip install -r requirements.txt)"
        )
        return _UNAVAILABLE

    # Past all gates — attempt provider call
    prompt = build_interpret_prompt(request)
    ai_logger.info(
        "Util AI service [AI-4] provider call: model=%s objective=%s region=%s",
        model,
        request.selected_objective,
        request.region or "unset",
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model=model,
            max_tokens=_MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_text: str = message.content[0].text
        ai_logger.info("Util AI service [AI-5] provider response received: chars=%s", len(raw_text))
    except anthropic.AuthenticationError:
        # Never log the key value.
        ai_logger.error("Util AI service [AI-6] exit: authentication failed — check ANTHROPIC_API_KEY value")
        return _ERROR
    except anthropic.RateLimitError:
        ai_logger.warning("Util AI service [AI-6] exit: provider rate limit hit")
        return _ERROR
    except anthropic.APIStatusError as exc:
        ai_logger.error(
            "Util AI service [AI-6] exit: API status error status=%s — "
            "check AI_SUMMARY_MODEL value (got: %s)",
            exc.status_code,
            model,
        )
        return _ERROR
    except Exception as exc:
        ai_logger.error("Util AI service [AI-6] exit: unexpected provider error type=%s", type(exc).__name__)
        return _ERROR

    try:
        data = _parse_ai_json(raw_text)
        ai_logger.info("Util AI service [AI-7] parse success")

        def _str_or_none(val: object) -> str | None:
            return str(val) if val not in (None, "null", "") else None

        return AiInterpretResponse(
            status="ok",
            # Primary display field.
            summary=_str_or_none(data.get("summary")),
            # Structured judgment fields.
            tradeoff_strength=_str_or_none(data.get("tradeoff_strength")),
            decision_confidence=_str_or_none(data.get("decision_confidence")),
            objective_driver=_str_or_none(data.get("objective_driver")),
            alternative_attractiveness=_str_or_none(data.get("alternative_attractiveness")),
            # Legacy sectioned fields — populated when old-style prompt output is returned.
            why_this_schedule=_str_or_none(data.get("why_this_schedule")),
            tradeoff_summary=_str_or_none(data.get("tradeoff_summary")),
            scenario_comparison=_str_or_none(data.get("scenario_comparison")),
            recommendation_memo=_str_or_none(data.get("recommendation_memo")),
        )
    except (json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
        ai_logger.error(
            "Util AI service [AI-7] exit: failed to parse model response type=%s",
            type(exc).__name__,
        )
        return _ERROR
