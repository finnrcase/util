from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pricing import PricingUnavailableError, get_price_series
from src.price_adapters.caiso import CaisoPricingError, fetch_caiso_day_ahead_prices
from src.price_adapters.ercot import _fetch_ercot_dam_table_cached, _iter_delivery_dates, fetch_ercot_prices
from src.price_adapters.pjm import _fetch_pjm_prices_cached, fetch_pjm_prices
from src.price_router import resolve_pricing_route


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
                "price_per_mwh": [120.0],
                "price_per_kwh": [0.12],
                "source_market": ["DAM"],
                "source_provider": ["CAISO"],
                "node_or_zone": ["TH_NP15_GEN-APND"],
                "is_forecast_or_historical": ["forecast"],
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
    assert result["price_per_mwh"].iloc[0] == pytest.approx(120.0)


def test_fetch_caiso_day_ahead_prices_fails_fast_by_default_on_429(monkeypatch) -> None:
    responses: list[object] = []

    class FakeResponse:
        def __init__(self):
            self.status_code = 429
            self.text = "<html><body><p>CAISO Acceptable Use Policy Violation.</p></body></html>"
            self.content = b""
            self.url = "https://example.test"

    def fake_requests_get(url, params, timeout):
        responses.append((url, params, timeout))
        return FakeResponse()

    monkeypatch.setattr("src.price_adapters.caiso.requests.get", fake_requests_get)
    monkeypatch.setattr("src.price_adapters.caiso.time.sleep", lambda seconds: None)

    with pytest.raises(CaisoPricingError, match="status 429"):
        fetch_caiso_day_ahead_prices(
            price_node="TH_NP15_GEN-APND",
            region_code="CAISO",
            start_time=pd.Timestamp("2026-03-22 00:00:00"),
            end_time=pd.Timestamp("2026-03-22 05:00:00"),
        )

    assert len(responses) == 1


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


def test_resolve_pricing_route_routes_ldwp_to_caiso_proxy() -> None:
    route = resolve_pricing_route("LDWP")

    assert route.provider_key == "caiso"
    assert route.source_provider == "CAISO"
    assert route.node_or_zone == "TH_SP15_GEN-APND"


def test_resolve_pricing_route_maps_california_utility_aliases_to_caiso() -> None:
    cases = {
        "PG&E": "TH_NP15_GEN-APND",
        "Pacific Gas and Electric": "TH_NP15_GEN-APND",
        "SCE": "TH_SP15_GEN-APND",
        "Southern California Edison": "TH_SP15_GEN-APND",
        "SDG&E": "TH_SP15_GEN-APND",
        "San Diego Gas and Electric": "TH_SP15_GEN-APND",
        "LADWP": "TH_SP15_GEN-APND",
    }

    for region_code, expected_node in cases.items():
        route = resolve_pricing_route(region_code)
        assert route.provider_key == "caiso"
        assert route.source_provider == "CAISO"
        assert route.node_or_zone == expected_node


def test_resolve_pricing_route_scaffolds_ercot() -> None:
    route = resolve_pricing_route("ERCOT_NORTH")

    assert route.provider_key == "ercot"
    assert route.source_provider == "ERCOT"


def test_get_price_series_is_transparent_for_scaffolded_ercot() -> None:
    with pytest.raises(PricingUnavailableError, match="ERCOT"):
        get_price_series(
            region_code="ERCOT_NORTH",
            start_time=pd.Timestamp("2026-03-21 00:00:00"),
            end_time=pd.Timestamp("2026-03-21 05:00:00"),
        )


def test_resolve_pricing_route_maps_ercot_easttx_to_houston_load_zone() -> None:
    route = resolve_pricing_route("ERCOT_EASTTX")

    assert route.provider_key == "ercot"
    assert route.source_provider == "ERCOT"
    assert route.node_or_zone == "LZ_HOUSTON"


def test_fetch_ercot_prices_normalizes_day_ahead_houston_zone_data(monkeypatch) -> None:
    _fetch_ercot_dam_table_cached.cache_clear()

    class FakeResponse:
        def __init__(self) -> None:
            self.status_code = 200
            self.url = "https://www.ercot.com/content/cdr/html/20260321_dam_spp.html"
            self.text = """
                <html>
                <body>
                    <table>
                        <tr><th>Oper Day</th><th>Hour Ending</th><th>HB_HOUSTON</th><th>LZ_HOUSTON</th></tr>
                        <tr><td>03/21/2026</td><td>01</td><td>28.0</td><td>31.5</td></tr>
                        <tr><td>03/21/2026</td><td>02</td><td>29.0</td><td>33.0</td></tr>
                    </table>
                </body>
                </html>
            """

    monkeypatch.setattr("src.price_adapters.ercot.requests.get", lambda url, timeout: FakeResponse())

    result = fetch_ercot_prices(
        region_code="ERCOT_EASTTX",
        node_or_zone="LZ_HOUSTON",
        start_time=pd.Timestamp("2026-03-21 00:00:00"),
        end_time=pd.Timestamp("2026-03-21 02:00:00"),
        market="DAM",
    )

    assert list(result["source_provider"].unique()) == ["ERCOT"]
    assert list(result["source_market"].unique()) == ["DAM"]
    assert list(result["node_or_zone"].unique()) == ["LZ_HOUSTON"]
    assert result["price_per_mwh"].iloc[0] == pytest.approx(31.5)
    assert result["price_per_kwh"].iloc[1] == pytest.approx(0.033)


def test_iter_delivery_dates_does_not_overshoot_next_ercot_day_for_partial_hour_end() -> None:
    delivery_dates = _iter_delivery_dates(
        pd.Timestamp("2026-03-26 22:10:00"),
        pd.Timestamp("2026-03-27 21:55:00"),
    )

    assert [value.isoformat() for value in delivery_dates] == ["2026-03-27"]


def test_get_price_series_surfaces_ercot_http_failures(monkeypatch) -> None:
    _fetch_ercot_dam_table_cached.cache_clear()

    class FakeResponse:
        def __init__(self) -> None:
            self.status_code = 404
            self.url = "https://www.ercot.com/content/cdr/html/20260322_dam_spp.html"
            self.text = "Not Found"

    monkeypatch.setattr("src.price_adapters.ercot.requests.get", lambda url, timeout: FakeResponse())

    with pytest.raises(PricingUnavailableError, match="status 404"):
        get_price_series(
            region_code="ERCOT_EASTTX",
            start_time=pd.Timestamp("2026-03-22 00:00:00"),
            end_time=pd.Timestamp("2026-03-22 05:00:00"),
        )


def test_fetch_ercot_prices_keeps_available_live_day_when_next_day_page_is_unpublished(monkeypatch) -> None:
    _fetch_ercot_dam_table_cached.cache_clear()

    class FakeResponse:
        def __init__(self, url: str, text: str) -> None:
            self.status_code = 200
            self.url = url
            self.text = text

    def fake_requests_get(url, timeout):
        if "20260327" in url:
            return FakeResponse(
                url,
                """
                    <html>
                    <body>
                        <table>
                            <tr><th>Oper Day</th><th>Hour Ending</th><th>LZ_HOUSTON</th></tr>
                            <tr><td>03/27/2026</td><td>23</td><td>42.0</td></tr>
                            <tr><td>03/27/2026</td><td>24</td><td>43.0</td></tr>
                        </table>
                    </body>
                    </html>
                """,
            )
        return FakeResponse(url, "<html><body><p>DAM results not yet posted.</p></body></html>")

    monkeypatch.setattr("src.price_adapters.ercot.requests.get", fake_requests_get)

    result = fetch_ercot_prices(
        region_code="ERCOT_EASTTX",
        node_or_zone="LZ_HOUSTON",
        start_time=pd.Timestamp("2026-03-26 22:10:00"),
        end_time=pd.Timestamp("2026-03-27 22:10:00"),
        market="DAM",
    )

    assert not result.empty
    assert list(result["node_or_zone"].unique()) == ["LZ_HOUSTON"]
    assert result["price_per_mwh"].max() == pytest.approx(43.0)


def test_resolve_pricing_route_maps_pjm_chicago_to_comed_zone() -> None:
    route = resolve_pricing_route("PJM_CHICAGO")

    assert route.provider_key == "pjm"
    assert route.source_provider == "PJM"
    assert route.node_or_zone == "COMED"


def test_fetch_pjm_prices_normalizes_day_ahead_zone_data(monkeypatch) -> None:
    monkeypatch.setenv("PJM_SUBSCRIPTION_KEY", "test-key")
    _fetch_pjm_prices_cached.cache_clear()

    class FakeResponse:
        def __init__(self) -> None:
            self.status_code = 200
            self.url = "https://api.pjm.com/api/v1/da_hrl_lmps?zone=COMED"
            self.text = (
                "datetime_beginning_utc,datetime_beginning_ept,pnode_name,type,zone,total_lmp_da,row_is_current\n"
                "2026-03-21T05:00:00Z,2026-03-21 01:00:00,COMED,ZONE,COMED,38.5,TRUE\n"
                "2026-03-21T06:00:00Z,2026-03-21 02:00:00,COMED,ZONE,COMED,41.0,TRUE\n"
            )

    def fake_requests_get(url, params, headers, timeout):
        assert url == "https://api.pjm.com/api/v1/da_hrl_lmps"
        assert params["zone"] == "COMED"
        assert params["type"] == "ZONE"
        assert params["format"] == "csv"
        assert headers["Ocp-Apim-Subscription-Key"] == "test-key"
        return FakeResponse()

    monkeypatch.setattr("src.price_adapters.pjm.requests.get", fake_requests_get)

    result = fetch_pjm_prices(
        region_code="PJM_CHICAGO",
        node_or_zone="COMED",
        start_time=pd.Timestamp("2026-03-21 00:00:00"),
        end_time=pd.Timestamp("2026-03-21 05:00:00"),
        market="DAY_AHEAD",
    )

    assert list(result["source_provider"].unique()) == ["PJM"]
    assert list(result["source_market"].unique()) == ["DAY_AHEAD"]
    assert list(result["node_or_zone"].unique()) == ["COMED"]
    assert result["price_per_mwh"].iloc[0] == pytest.approx(38.5)
    assert result["price_per_kwh"].iloc[1] == pytest.approx(0.041)


def test_get_price_series_surfaces_pjm_http_failures(monkeypatch) -> None:
    monkeypatch.setenv("PJM_SUBSCRIPTION_KEY", "test-key")
    _fetch_pjm_prices_cached.cache_clear()

    class FakeResponse:
        def __init__(self) -> None:
            self.status_code = 403
            self.url = "https://api.pjm.com/api/v1/da_hrl_lmps?zone=COMED"
            self.text = "Forbidden"

    monkeypatch.setattr(
        "src.price_adapters.pjm.requests.get",
        lambda url, params, headers, timeout: FakeResponse(),
    )

    with pytest.raises(PricingUnavailableError, match="status 403"):
        get_price_series(
            region_code="PJM_CHICAGO",
            start_time=pd.Timestamp("2026-03-22 00:00:00"),
            end_time=pd.Timestamp("2026-03-22 05:00:00"),
        )
