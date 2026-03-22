from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pricing import get_price_series
from src.price_adapters.caiso import CaisoPricingError, fetch_caiso_day_ahead_prices


def test_get_price_series_passes_start_and_end_time(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_caiso_day_ahead_prices(
        *,
        price_node: str,
        region_code: str,
        start_time,
        end_time,
        market_run_id: str,
    ) -> pd.DataFrame:
        captured["price_node"] = price_node
        captured["region_code"] = region_code
        captured["start_time"] = start_time
        captured["end_time"] = end_time
        captured["market_run_id"] = market_run_id
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2026-03-21 00:00:00"]),
                "price_per_kwh": [0.12],
                "source": ["CAISO"],
                "region_code": ["CAISO"],
                "price_node": ["TH_NP15_GEN-APND"],
            }
        )

    monkeypatch.setattr("src.pricing.fetch_caiso_day_ahead_prices", fake_fetch_caiso_day_ahead_prices)

    result = get_price_series(
        region_code="CAISO_NORTH",
        start_time=pd.Timestamp("2026-03-21 00:00:00"),
        end_time=pd.Timestamp("2026-03-21 05:00:00"),
    )

    assert len(result) == 1
    assert captured["start_time"] == pd.Timestamp("2026-03-21 00:00:00")
    assert captured["end_time"] == pd.Timestamp("2026-03-21 05:00:00")


def test_fetch_caiso_day_ahead_prices_retries_on_429(monkeypatch) -> None:
    responses: list[object] = []

    class FakeResponse:
        def __init__(self, status_code: int, text: str, content: bytes = b"", url: str = "https://example.test"):
            self.status_code = status_code
            self.text = text
            self.content = content
            self.url = url

    def fake_requests_get(url, params, timeout):
        responses.append((url, params, timeout))
        if len(responses) == 1:
            return FakeResponse(
                429,
                "<html><body><p>CAISO Acceptable Use Policy Violation. Please retry your request after 5 seconds.</p></body></html>",
            )
        return FakeResponse(200, "", content=b"fake-zip")

    def fake_extract_first_csv(zip_bytes: bytes) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "INTERVALSTARTTIME_GMT": ["2026-03-21T07:00:00Z"],
                "LMP_PRC": [120.0],
            }
        )

    monkeypatch.setattr("src.price_adapters.caiso.requests.get", fake_requests_get)
    monkeypatch.setattr("src.price_adapters.caiso.time.sleep", lambda seconds: None)
    monkeypatch.setattr("src.price_adapters.caiso._extract_first_csv", fake_extract_first_csv)

    result = fetch_caiso_day_ahead_prices(
        price_node="TH_NP15_GEN-APND",
        region_code="CAISO",
        start_time=pd.Timestamp("2026-03-21 00:00:00"),
        end_time=pd.Timestamp("2026-03-21 05:00:00"),
        max_retry_attempts=2,
        retry_sleep_seconds=0,
    )

    assert len(responses) == 2
    assert len(result) == 1
    assert result["price_per_kwh"].iloc[0] == pytest.approx(0.12)


def test_fetch_caiso_day_ahead_prices_raises_after_retry_exhausted(monkeypatch) -> None:
    class FakeResponse:
        def __init__(self):
            self.status_code = 429
            self.text = "<html><body><p>CAISO Acceptable Use Policy Violation. Please retry your request after 5 seconds.</p></body></html>"
            self.content = b""
            self.url = "https://example.test"

    monkeypatch.setattr("src.price_adapters.caiso.requests.get", lambda url, params, timeout: FakeResponse())
    monkeypatch.setattr("src.price_adapters.caiso.time.sleep", lambda seconds: None)

    with pytest.raises(CaisoPricingError, match="status 429"):
        fetch_caiso_day_ahead_prices(
            price_node="TH_NP15_GEN-APND",
            region_code="CAISO",
            start_time=pd.Timestamp("2026-03-22 00:00:00"),
            end_time=pd.Timestamp("2026-03-22 05:00:00"),
            max_retry_attempts=2,
            retry_sleep_seconds=0,
        )
