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
_PROVIDER_TIMEOUT_SECONDS = 30

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
    import time as _time
    t0 = _time.monotonic()

    # [AI-3] Resolve config — do this once so each gate can log its resolved value.
    enabled = _is_enabled()
    key_present = bool(_get_api_key())
    model = _get_model()

    ai_logger.info(
        "[AI-3-CONFIG] loaded: enabled=%s key_present=%s model=%s objective=%s elapsed=%.3fs",
        enabled,
        key_present,
        model,
        request.selected_objective,
        _time.monotonic() - t0,
    )
    ai_logger.info("[AI-4] ai_enabled=%s", enabled)
    ai_logger.info("[AI-5] anthropic_key_present=%s", key_present)
    ai_logger.info("[AI-6] model_configured=%s value=%s", bool(model), model)

    if not enabled:
        ai_logger.info("[AI-GATE] exit: AI_SUMMARY_ENABLED is false or unset elapsed=%.3fs", _time.monotonic() - t0)
        return _UNAVAILABLE

    api_key = _get_api_key()
    if not api_key:
        ai_logger.info("[AI-GATE] exit: ANTHROPIC_API_KEY missing or empty elapsed=%.3fs", _time.monotonic() - t0)
        return _UNAVAILABLE

    try:
        import anthropic
    except ImportError:
        ai_logger.warning(
            "[AI-GATE] exit: 'anthropic' package not installed — "
            "run: pip install anthropic elapsed=%.3fs",
            _time.monotonic() - t0,
        )
        return _UNAVAILABLE

    # [AI-7] Provider client
    try:
        client = anthropic.Anthropic(api_key=api_key)
        ai_logger.info("[AI-7] provider client created elapsed=%.3fs", _time.monotonic() - t0)
    except Exception as exc:
        ai_logger.error(
            "[AI-7] failed to create provider client: type=%s msg=%s elapsed=%.3fs",
            type(exc).__name__,
            str(exc)[:200],
            _time.monotonic() - t0,
        )
        return _ERROR

    # [AI-8] Build prompt and start provider request
    prompt = build_interpret_prompt(request)
    ai_logger.info(
        "[AI-8] provider request started: model=%s max_tokens=%s prompt_chars=%s elapsed=%.3fs",
        model,
        _MAX_TOKENS,
        len(prompt),
        _time.monotonic() - t0,
    )
    t_provider = _time.monotonic()

    try:
        message = client.messages.create(
            model=model,
            max_tokens=_MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
            timeout=_PROVIDER_TIMEOUT_SECONDS,
        )
        raw_text: str = message.content[0].text
        t_provider_elapsed = round(_time.monotonic() - t_provider, 3)

        # [AI-9] Provider request finished
        ai_logger.info(
            "[AI-9] provider request finished: response_chars=%s provider_elapsed=%.3fs total_elapsed=%.3fs",
            len(raw_text),
            t_provider_elapsed,
            _time.monotonic() - t0,
        )

    except anthropic.APITimeoutError:
        ai_logger.warning(
            "[AI-ERR] provider timeout: no response within %ss "
            "provider_elapsed=%.3fs total_elapsed=%.3fs",
            _PROVIDER_TIMEOUT_SECONDS,
            round(_time.monotonic() - t_provider, 3),
            round(_time.monotonic() - t0, 3),
        )
        return _UNAVAILABLE
    except anthropic.AuthenticationError:
        ai_logger.error(
            "[AI-ERR] authentication failed — check ANTHROPIC_API_KEY "
            "provider_elapsed=%.3fs total_elapsed=%.3fs",
            round(_time.monotonic() - t_provider, 3),
            round(_time.monotonic() - t0, 3),
        )
        return _ERROR
    except anthropic.RateLimitError:
        ai_logger.warning(
            "[AI-ERR] provider rate limit hit "
            "provider_elapsed=%.3fs total_elapsed=%.3fs",
            round(_time.monotonic() - t_provider, 3),
            round(_time.monotonic() - t0, 3),
        )
        return _ERROR
    except anthropic.APIStatusError as exc:
        ai_logger.error(
            "[AI-ERR] API status error: status=%s model=%s "
            "provider_elapsed=%.3fs total_elapsed=%.3fs",
            exc.status_code,
            model,
            round(_time.monotonic() - t_provider, 3),
            round(_time.monotonic() - t0, 3),
        )
        return _ERROR
    except Exception as exc:
        ai_logger.error(
            "[AI-ERR] unexpected provider error: type=%s msg=%s "
            "provider_elapsed=%.3fs total_elapsed=%.3fs",
            type(exc).__name__,
            str(exc)[:200],
            round(_time.monotonic() - t_provider, 3),
            round(_time.monotonic() - t0, 3),
        )
        return _ERROR

    try:
        data = _parse_ai_json(raw_text)

        def _str_or_none(val: object) -> str | None:
            return str(val) if val not in (None, "null", "") else None

        response = AiInterpretResponse(
            status="ok",
            summary=_str_or_none(data.get("summary")),
            tradeoff_strength=_str_or_none(data.get("tradeoff_strength")),
            decision_confidence=_str_or_none(data.get("decision_confidence")),
            objective_driver=_str_or_none(data.get("objective_driver")),
            alternative_attractiveness=_str_or_none(data.get("alternative_attractiveness")),
            why_this_schedule=_str_or_none(data.get("why_this_schedule")),
            tradeoff_summary=_str_or_none(data.get("tradeoff_summary")),
            scenario_comparison=_str_or_none(data.get("scenario_comparison")),
            recommendation_memo=_str_or_none(data.get("recommendation_memo")),
        )
        # [AI-10] Total elapsed
        ai_logger.info(
            "[AI-10] complete: status=ok total_elapsed=%.3fs",
            round(_time.monotonic() - t0, 3),
        )
        return response

    except (json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
        ai_logger.error(
            "[AI-ERR] JSON parse failed: type=%s total_elapsed=%.3fs",
            type(exc).__name__,
            round(_time.monotonic() - t0, 3),
        )
        return _ERROR
