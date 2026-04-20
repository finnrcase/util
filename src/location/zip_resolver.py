"""
ZIP code -> coordinate resolution for Util.

Lookup order (first hit wins):
  1. In-memory coordinate cache      — populated by any prior successful lookup
  2. Bundled local CSV dataset        — data/raw/us_zip_coords.csv (fast, no network)
  3. pgeocode (remote fallback)       — downloads GeoNames data on first cold use

Cold-start protection: tiers 1 and 2 never touch the network, so the first
request is instant as long as the bundled CSV covers the ZIP. Tier 3 fires
only for ZIPs not in the CSV, with a timeout + short-delay retry.

Timeout errors from tier 3 are re-raised as TimeoutError so callers can
translate them to a 503 response rather than a raw 500.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from functools import lru_cache
import logging
import threading
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pgeocode


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ZIP_RESOLUTION_TIMEOUT_SECONDS = 20
ZIP_RESOLUTION_MAX_ATTEMPTS = 2
_ZIP_RETRY_DELAY_SECONDS = 1.5

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_LOCAL_ZIP_CSV_PATH = _PROJECT_ROOT / "data" / "raw" / "us_zip_coords.csv"

_zip_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="util_zip_lookup")
_coordinate_cache_lock = threading.Lock()
_coordinate_cache: dict[tuple[str, str], dict[str, Any]] = {}

zip_logger = logging.getLogger("uvicorn.error")


# ---------------------------------------------------------------------------
# Tier 1: In-memory coordinate cache
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Tier 2: Bundled local CSV dataset
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_local_zip_dataset() -> pd.DataFrame | None:
    """
    Load the bundled US ZIP coordinate CSV into a DataFrame.

    Keyed on (postal_code, country_code). Returns None if the file does not
    exist or cannot be parsed — callers fall through to tier 3 silently.

    Run scripts/generate_zip_coords.py to create the full dataset.
    The sample file shipped with the repo covers the most common US ZIPs.
    """
    if not _LOCAL_ZIP_CSV_PATH.exists():
        zip_logger.warning(
            "[ZIP-LOCAL] Bundled ZIP dataset not found at %s — tier-2 lookup disabled. "
            "Run scripts/generate_zip_coords.py to generate the full dataset.",
            _LOCAL_ZIP_CSV_PATH,
        )
        return None
    try:
        df = pd.read_csv(
            _LOCAL_ZIP_CSV_PATH,
            dtype={"postal_code": str, "country_code": str},
        )
        required = {"postal_code", "country_code", "latitude", "longitude"}
        missing = required - set(df.columns)
        if missing:
            zip_logger.error(
                "[ZIP-LOCAL] Bundled ZIP dataset is missing columns: %s — tier-2 lookup disabled.",
                missing,
            )
            return None
        df["postal_code"] = df["postal_code"].str.strip().str.zfill(5)
        df["country_code"] = df["country_code"].str.strip().str.upper()
        df = df.dropna(subset=["latitude", "longitude"])
        df = df.set_index(["postal_code", "country_code"])
        zip_logger.info(
            "[ZIP-LOCAL] Loaded %d rows from bundled ZIP dataset %s",
            len(df),
            _LOCAL_ZIP_CSV_PATH,
        )
        return df
    except Exception:
        zip_logger.exception(
            "[ZIP-LOCAL] Failed to load bundled ZIP dataset %s — tier-2 lookup disabled.",
            _LOCAL_ZIP_CSV_PATH,
        )
        return None


def _lookup_local_dataset(zip_code: str, country_code: str) -> dict[str, Any] | None:
    """Return coordinates from the bundled CSV, or None on miss/error."""
    df = _load_local_zip_dataset()
    if df is None:
        return None
    key = (str(zip_code).strip().zfill(5), str(country_code).strip().upper())
    try:
        row = df.loc[key]
    except KeyError:
        return None
    # If the index matches multiple rows (duplicate ZIP), take the first.
    if isinstance(row, pd.DataFrame):
        row = row.iloc[0]
    lat = row["latitude"]
    lon = row["longitude"]
    if pd.isna(lat) or pd.isna(lon):
        return None
    return {
        "zip_code": zip_code,
        "latitude": float(lat),
        "longitude": float(lon),
    }


# ---------------------------------------------------------------------------
# Tier 3: pgeocode (remote fallback)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=4)
def _get_nominatim(country_code: str) -> pgeocode.Nominatim:
    zip_logger.info("[ZIP-REMOTE] Initializing pgeocode dataset for country=%s", country_code)
    return pgeocode.Nominatim(country_code)


def warm_zip_lookup(country_code: str = "US") -> None:
    """
    Pre-load the bundled local dataset and (optionally) pgeocode.

    Called from the /api/v1/warmup endpoint so cold-start latency is paid
    at server boot rather than on the first user request.
    """
    started_at = time.perf_counter()
    _load_local_zip_dataset()
    try:
        _get_nominatim(country_code)
    except Exception:
        zip_logger.warning(
            "[ZIP-REMOTE] pgeocode warmup failed for country=%s — remote fallback may be slow.",
            country_code,
        )
    zip_logger.info(
        "[ZIP-LOCAL] Warmup complete country=%s elapsed_ms=%.1f",
        country_code,
        (time.perf_counter() - started_at) * 1000.0,
    )


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
            f"ZIP coordinate lookup timed out after {timeout_seconds}s for ZIP {zip_code}. "
            "Add this ZIP to data/raw/us_zip_coords.csv or run scripts/generate_zip_coords.py."
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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def zip_to_coordinates(zip_code: str, country_code: str = "US") -> dict[str, Any]:
    """
    Resolve a ZIP code to latitude/longitude.

    Returns a dict with keys: zip_code, latitude, longitude, and internal
    metadata keys _lookup_source, _retry_used, _lookup_duration_ms,
    _country_code.

    Raises
    ------
    TimeoutError
        If the remote pgeocode lookup exceeds the timeout on all attempts.
        Callers should map this to a 503 response, not a 500.
    ValueError
        If the ZIP is not recognised by any lookup tier.
    """
    zip_code = str(zip_code).strip()
    country_code = str(country_code).strip().upper()
    started_at = time.perf_counter()

    # ── Tier 1: in-memory cache ────────────────────────────────────────────
    cached = _get_cached_coordinates(zip_code, country_code)
    if cached is not None:
        elapsed_ms = (time.perf_counter() - started_at) * 1000.0
        zip_logger.info(
            "[ZIP-CACHE] Hit zip=%s country=%s elapsed_ms=%.1f",
            zip_code, country_code, elapsed_ms,
        )
        return {
            **cached,
            "_lookup_source": "cache",
            "_retry_used": False,
            "_lookup_duration_ms": elapsed_ms,
            "_country_code": country_code,
        }

    zip_logger.info("[ZIP-CACHE] Miss zip=%s country=%s", zip_code, country_code)

    # ── Tier 2: bundled local CSV dataset ─────────────────────────────────
    local = _lookup_local_dataset(zip_code, country_code)
    if local is not None:
        _store_cached_coordinates(zip_code, country_code, local)
        elapsed_ms = (time.perf_counter() - started_at) * 1000.0
        zip_logger.info(
            "[ZIP-LOCAL] Hit zip=%s country=%s lat=%s lon=%s elapsed_ms=%.1f",
            zip_code, country_code, local["latitude"], local["longitude"], elapsed_ms,
        )
        return {
            **local,
            "_lookup_source": "local_dataset",
            "_retry_used": False,
            "_lookup_duration_ms": elapsed_ms,
            "_country_code": country_code,
        }

    zip_logger.info(
        "[ZIP-LOCAL] Miss zip=%s country=%s — falling through to remote lookup",
        zip_code, country_code,
    )

    # ── Tier 3: pgeocode remote fallback ──────────────────────────────────
    last_error: Exception | None = None
    retry_used = False

    for attempt in range(1, ZIP_RESOLUTION_MAX_ATTEMPTS + 1):
        attempt_started_at = time.perf_counter()
        source_label = "remote_first" if attempt == 1 else "remote_retry"
        if attempt > 1:
            retry_used = True
            zip_logger.info(
                "[ZIP-REMOTE] Waiting %.1fs before retry zip=%s",
                _ZIP_RETRY_DELAY_SECONDS, zip_code,
            )
            time.sleep(_ZIP_RETRY_DELAY_SECONDS)

        zip_logger.info(
            "[ZIP-REMOTE] Lookup start zip=%s country=%s source=%s attempt=%d timeout_s=%d",
            zip_code, country_code, source_label, attempt, ZIP_RESOLUTION_TIMEOUT_SECONDS,
        )

        try:
            result = _query_postal_code_with_timeout(zip_code, country_code, ZIP_RESOLUTION_TIMEOUT_SECONDS)
            resolved = _resolve_coordinates_from_result(zip_code, result)
            _store_cached_coordinates(zip_code, country_code, resolved)

            total_elapsed_ms = (time.perf_counter() - started_at) * 1000.0
            attempt_elapsed_ms = (time.perf_counter() - attempt_started_at) * 1000.0
            zip_logger.info(
                "[ZIP-REMOTE] Success zip=%s country=%s source=%s attempt=%d retry_used=%s "
                "attempt_ms=%.1f total_ms=%.1f lat=%s lon=%s",
                zip_code, country_code, source_label, attempt, retry_used,
                attempt_elapsed_ms, total_elapsed_ms,
                resolved["latitude"], resolved["longitude"],
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
                "[ZIP-REMOTE] Timeout zip=%s country=%s source=%s attempt=%d attempt_ms=%.1f",
                zip_code, country_code, source_label, attempt,
                (time.perf_counter() - attempt_started_at) * 1000.0,
            )

        except Exception as exc:
            last_error = exc
            zip_logger.exception(
                "[ZIP-REMOTE] Failure zip=%s country=%s source=%s attempt=%d type=%s",
                zip_code, country_code, source_label, attempt, type(exc).__name__,
            )
            break  # non-timeout errors don't benefit from retry

    total_elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    if isinstance(last_error, TimeoutError):
        zip_logger.error(
            "[ZIP-REMOTE] Giving up after timeout zip=%s country=%s attempts=%d total_ms=%.1f",
            zip_code, country_code, ZIP_RESOLUTION_MAX_ATTEMPTS, total_elapsed_ms,
        )
        raise TimeoutError(
            f"ZIP coordinate lookup timed out after {ZIP_RESOLUTION_TIMEOUT_SECONDS}s "
            f"for ZIP {zip_code} (tried {ZIP_RESOLUTION_MAX_ATTEMPTS} times). "
            "Add this ZIP to data/raw/us_zip_coords.csv or run scripts/generate_zip_coords.py."
        ) from last_error

    zip_logger.error(
        "[ZIP-REMOTE] Giving up after failure zip=%s country=%s attempts=%d total_ms=%.1f type=%s",
        zip_code, country_code, ZIP_RESOLUTION_MAX_ATTEMPTS, total_elapsed_ms,
        type(last_error).__name__ if last_error is not None else "UnknownError",
    )
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Unexpected ZIP lookup failure for ZIP code {zip_code}")


# ---------------------------------------------------------------------------
# Place label (uses pgeocode directly — not on the hot path)
# ---------------------------------------------------------------------------

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
