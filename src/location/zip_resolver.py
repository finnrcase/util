"""
ZIP code -> coordinate resolution for Util.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from functools import lru_cache
import logging
import threading
import time
from typing import Any

import pandas as pd
import pgeocode


ZIP_RESOLUTION_TIMEOUT_SECONDS = 20
ZIP_RESOLUTION_MAX_ATTEMPTS = 2
_zip_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="util_zip_lookup")
_coordinate_cache_lock = threading.Lock()
_coordinate_cache: dict[tuple[str, str], dict[str, Any]] = {}
zip_logger = logging.getLogger("uvicorn.error")


@lru_cache(maxsize=4)
def _get_nominatim(country_code: str) -> pgeocode.Nominatim:
    zip_logger.info("Util ZIP lookup: initializing pgeocode dataset for country=%s", country_code)
    return pgeocode.Nominatim(country_code)


def warm_zip_lookup(country_code: str = "US") -> None:
    """Warm the pgeocode dataset for the given country."""
    started_at = time.perf_counter()
    _get_nominatim(country_code)
    zip_logger.info(
        "Util ZIP lookup: warmup complete country=%s elapsed_ms=%.1f",
        country_code,
        (time.perf_counter() - started_at) * 1000.0,
    )


def _cache_key(zip_code: str, country_code: str) -> tuple[str, str]:
    return (str(zip_code).strip(), str(country_code).strip().upper())


def _get_cached_coordinates(zip_code: str, country_code: str) -> dict[str, Any] | None:
    key = _cache_key(zip_code, country_code)
    with _coordinate_cache_lock:
        cached = _coordinate_cache.get(key)
        return dict(cached) if cached is not None else None


def _store_cached_coordinates(zip_code: str, country_code: str, resolved: dict[str, Any]) -> None:
    key = _cache_key(zip_code, country_code)
    cache_value = {
        "zip_code": resolved["zip_code"],
        "latitude": resolved["latitude"],
        "longitude": resolved["longitude"],
    }
    with _coordinate_cache_lock:
        _coordinate_cache[key] = cache_value


def _query_postal_code(zip_code: str, country_code: str) -> Any:
    nomi = _get_nominatim(country_code)
    return nomi.query_postal_code(zip_code)


def _query_postal_code_with_timeout(zip_code: str, country_code: str, timeout_seconds: int) -> Any:
    future = _zip_executor.submit(_query_postal_code, zip_code, country_code)
    try:
        return future.result(timeout=timeout_seconds)
    except FutureTimeoutError as exc:
        future.cancel()
        raise TimeoutError(
            f"ZIP coordinate lookup timed out after {timeout_seconds} seconds for ZIP code {zip_code}."
        ) from exc


def _resolve_coordinates_from_result(zip_code: str, result: Any) -> dict[str, Any]:
    latitude = result.latitude
    longitude = result.longitude

    if pd.isna(latitude) or pd.isna(longitude):
        raise ValueError(f"Could not determine coordinates for ZIP code: {zip_code}")

    return {
        "zip_code": zip_code,
        "latitude": float(latitude),
        "longitude": float(longitude),
    }


def zip_to_coordinates(zip_code: str, country_code: str = "US") -> dict[str, Any]:
    zip_code = str(zip_code).strip()
    country_code = str(country_code).strip().upper()
    started_at = time.perf_counter()

    cached = _get_cached_coordinates(zip_code, country_code)
    if cached is not None:
        elapsed_ms = (time.perf_counter() - started_at) * 1000.0
        zip_logger.info(
            "Util ZIP lookup: cache hit zip=%s country=%s elapsed_ms=%.1f",
            zip_code,
            country_code,
            elapsed_ms,
        )
        return {
            **cached,
            "_lookup_source": "cache",
            "_retry_used": False,
            "_lookup_duration_ms": elapsed_ms,
            "_country_code": country_code,
        }

    zip_logger.info("Util ZIP lookup: cache miss zip=%s country=%s", zip_code, country_code)

    last_error: Exception | None = None
    retry_used = False

    for attempt in range(1, ZIP_RESOLUTION_MAX_ATTEMPTS + 1):
        attempt_started_at = time.perf_counter()
        source_label = "first_attempt" if attempt == 1 else "retry"
        if attempt > 1:
            retry_used = True

        zip_logger.info(
            "Util ZIP lookup: external lookup start zip=%s country=%s source=%s attempt=%s timeout_s=%s",
            zip_code,
            country_code,
            source_label,
            attempt,
            ZIP_RESOLUTION_TIMEOUT_SECONDS,
        )

        try:
            result = _query_postal_code_with_timeout(zip_code, country_code, ZIP_RESOLUTION_TIMEOUT_SECONDS)
            resolved = _resolve_coordinates_from_result(zip_code, result)
            _store_cached_coordinates(zip_code, country_code, resolved)

            total_elapsed_ms = (time.perf_counter() - started_at) * 1000.0
            attempt_elapsed_ms = (time.perf_counter() - attempt_started_at) * 1000.0
            zip_logger.info(
                "Util ZIP lookup: success zip=%s country=%s source=%s attempt=%s retry_used=%s attempt_elapsed_ms=%.1f total_elapsed_ms=%.1f latitude=%s longitude=%s",
                zip_code,
                country_code,
                source_label,
                attempt,
                retry_used,
                attempt_elapsed_ms,
                total_elapsed_ms,
                resolved["latitude"],
                resolved["longitude"],
            )
            return {
                **resolved,
                "_lookup_source": source_label,
                "_retry_used": retry_used,
                "_lookup_duration_ms": total_elapsed_ms,
                "_country_code": country_code,
            }
        except TimeoutError as exc:
            last_error = exc
            zip_logger.warning(
                "Util ZIP lookup: timeout zip=%s country=%s source=%s attempt=%s retry_used=%s attempt_elapsed_ms=%.1f msg=%s",
                zip_code,
                country_code,
                source_label,
                attempt,
                retry_used,
                (time.perf_counter() - attempt_started_at) * 1000.0,
                str(exc),
            )
        except Exception as exc:
            last_error = exc
            zip_logger.exception(
                "Util ZIP lookup: failure zip=%s country=%s source=%s attempt=%s retry_used=%s attempt_elapsed_ms=%.1f type=%s",
                zip_code,
                country_code,
                source_label,
                attempt,
                retry_used,
                (time.perf_counter() - attempt_started_at) * 1000.0,
                type(exc).__name__,
            )
            break

    total_elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    if isinstance(last_error, TimeoutError):
        zip_logger.error(
            "Util ZIP lookup: giving up after timeout zip=%s country=%s attempts=%s total_elapsed_ms=%.1f",
            zip_code,
            country_code,
            ZIP_RESOLUTION_MAX_ATTEMPTS,
            total_elapsed_ms,
        )
        raise TimeoutError(
            f"ZIP coordinate lookup timed out after {ZIP_RESOLUTION_TIMEOUT_SECONDS} seconds for ZIP code {zip_code}."
        ) from last_error

    zip_logger.error(
        "Util ZIP lookup: giving up after failure zip=%s country=%s attempts=%s total_elapsed_ms=%.1f type=%s",
        zip_code,
        country_code,
        ZIP_RESOLUTION_MAX_ATTEMPTS,
        total_elapsed_ms,
        type(last_error).__name__ if last_error is not None else "UnknownError",
    )
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Unexpected ZIP lookup failure for ZIP code {zip_code}")


def zip_to_place_label(zip_code: str, country_code: str = "US") -> str:
    zip_code = str(zip_code).strip()
    country_code = str(country_code).strip().upper()

    result = _query_postal_code_with_timeout(zip_code, country_code, ZIP_RESOLUTION_TIMEOUT_SECONDS)

    place_name = getattr(result, "place_name", None)
    state_code = getattr(result, "state_code", None)

    if pd.isna(place_name) or not str(place_name).strip():
        raise ValueError(f"Could not determine place name for ZIP code: {zip_code}")

    place_label = str(place_name).strip()
    if not pd.isna(state_code) and str(state_code).strip():
        place_label = f"{place_label}, {str(state_code).strip()}"

    return place_label
