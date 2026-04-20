from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.location import zip_resolver


@pytest.fixture(autouse=True)
def clear_zip_cache() -> None:
    zip_resolver._coordinate_cache.clear()
    zip_resolver._get_nominatim.cache_clear()


def test_zip_to_coordinates_uses_cache(monkeypatch) -> None:
    calls = {"count": 0}

    def fake_query(zip_code: str, country_code: str, timeout_seconds: int):
        calls["count"] += 1
        return SimpleNamespace(latitude=34.05, longitude=-118.24)

    monkeypatch.setattr(zip_resolver, "_query_postal_code_with_timeout", fake_query)

    first = zip_resolver.zip_to_coordinates("90012", "US")
    second = zip_resolver.zip_to_coordinates("90012", "US")

    assert calls["count"] == 1
    assert first["_lookup_source"] == "first_attempt"
    assert second["_lookup_source"] == "cache"
    assert second["latitude"] == pytest.approx(34.05)
    assert second["longitude"] == pytest.approx(-118.24)


def test_zip_to_coordinates_retries_once_after_timeout(monkeypatch) -> None:
    calls = {"count": 0}

    def fake_query(zip_code: str, country_code: str, timeout_seconds: int):
        calls["count"] += 1
        if calls["count"] == 1:
            raise TimeoutError("first attempt timed out")
        return SimpleNamespace(latitude=34.05, longitude=-118.24)

    monkeypatch.setattr(zip_resolver, "_query_postal_code_with_timeout", fake_query)

    resolved = zip_resolver.zip_to_coordinates("90012", "US")

    assert calls["count"] == 2
    assert resolved["_lookup_source"] == "retry"
    assert resolved["_retry_used"] is True
    assert resolved["zip_code"] == "90012"


def test_zip_to_coordinates_raises_after_retry_timeout(monkeypatch) -> None:
    calls = {"count": 0}

    def fake_query(zip_code: str, country_code: str, timeout_seconds: int):
        calls["count"] += 1
        raise TimeoutError("still timed out")

    monkeypatch.setattr(zip_resolver, "_query_postal_code_with_timeout", fake_query)

    with pytest.raises(TimeoutError):
        zip_resolver.zip_to_coordinates("90012", "US")

    assert calls["count"] == zip_resolver.ZIP_RESOLUTION_MAX_ATTEMPTS
