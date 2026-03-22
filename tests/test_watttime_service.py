from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.watttime_service import _fetch_json, get_token


def test_fetch_json_refreshes_cached_token_on_401(monkeypatch) -> None:
    get_token.cache_clear()

    token_calls: list[str] = []
    request_calls: list[str] = []

    class FakeResponse:
        def __init__(self, status_code: int, payload: dict, content_type: str = "application/json"):
            self.status_code = status_code
            self._payload = payload
            self.headers = {"content-type": content_type}
            self.url = "https://example.test"
            self.history = []
            self.text = str(payload)

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"http {self.status_code}")

    def fake_get_token() -> str:
        token = f"token-{len(token_calls) + 1}"
        token_calls.append(token)
        return token
    fake_get_token.cache_clear = lambda: None  # type: ignore[attr-defined]

    def fake_requests_get(url, headers, params, timeout):
        request_calls.append(headers["Authorization"])
        if len(request_calls) == 1:
            return FakeResponse(401, {"error": "expired"})
        return FakeResponse(200, {"data": [{"point_time": "2026-03-22T00:00:00Z", "value": 123.0}]})

    monkeypatch.setattr("services.watttime_service.get_token", fake_get_token)
    monkeypatch.setattr("services.watttime_service.requests.get", fake_requests_get)

    payload = _fetch_json("https://example.test", {"region": "CAISO_NORTH"})

    assert payload["data"][0]["value"] == 123.0
    assert len(token_calls) == 2
    assert request_calls == ["Bearer token-1", "Bearer token-2"]


def test_fetch_json_raises_on_repeated_401(monkeypatch) -> None:
    get_token.cache_clear()

    class FakeResponse:
        def __init__(self):
            self.status_code = 401
            self.headers = {"content-type": "application/json"}
            self.url = "https://example.test"
            self.history = []
            self.text = '{"error":"expired"}'

        def json(self):
            return {"error": "expired"}

        def raise_for_status(self):
            raise RuntimeError("http 401")

    fake_get_token = lambda: "token"
    fake_get_token.cache_clear = lambda: None  # type: ignore[attr-defined]

    monkeypatch.setattr("services.watttime_service.get_token", fake_get_token)
    monkeypatch.setattr("services.watttime_service.requests.get", lambda url, headers, params, timeout: FakeResponse())

    with pytest.raises(ValueError, match="unauthorized"):
        _fetch_json("https://example.test", {"region": "CAISO_NORTH"})
