from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.api.main import ALLOWED_HEADERS, ALLOWED_METHODS, ALLOWED_ORIGINS, app
from src.api.schemas import ExportResponse


client = TestClient(app)
VERCEL_ORIGIN = "https://util-ten-delta.vercel.app"


def _fake_result() -> dict:
    timestamps = pd.to_datetime(
        [
            "2026-03-27T00:00:00",
            "2026-03-27T00:05:00",
            "2026-03-27T00:10:00",
        ]
    )
    forecast = pd.DataFrame(
        {
            "timestamp": timestamps,
            "carbon_g_per_kwh": [100.0, 95.0, 90.0],
            "price_per_kwh": [0.03, 0.03, 0.04],
            "carbon_source": ["live_forecast", "live_forecast", "live_forecast"],
            "price_signal_source": ["live_forecast", "live_forecast", "live_forecast"],
            "pricing_status": ["live_market", "live_market", "live_market"],
            "pricing_message": ["Using live CAISO pricing routed from the resolved WattTime region."] * 3,
            "pricing_source": ["CAISO"] * 3,
            "pricing_market": ["DAM"] * 3,
            "pricing_region_code": ["LDWP"] * 3,
            "pricing_node": ["TH_SP15_GEN-APND"] * 3,
            "forecast_region_used": ["LDWP"] * 3,
        }
    )
    optimized = forecast.copy()
    optimized["eligible_flag"] = [1, 1, 1]
    optimized["run_flag"] = [1, 1, 0]
    optimized["score"] = optimized["price_per_kwh"]

    schedule = pd.DataFrame(
        {
            "timestamp": timestamps,
            "eligible_flag": [1, 1, 1],
            "run_flag": [1, 1, 0],
            "recommended_action": ["Run", "Run", "Wait"],
            "price_per_kwh": [0.03, 0.03, 0.04],
            "carbon_g_per_kwh": [100.0, 95.0, 90.0],
        }
    )
    baseline = forecast.copy()
    baseline["baseline_run_flag"] = [1, 1, 0]

    workload = type(
        "Workload",
        (),
        {
            "zip_code": "90012",
            "compute_hours_required": 1,
            "deadline": datetime.fromisoformat("2026-03-27T06:00:00"),
            "objective": "cost",
            "machine_watts": 1000,
            "carbon_weight": 0.5,
            "price_weight": 0.5,
        },
    )()

    return {
        "workload_input": workload,
        "region": "LDWP",
        "location_info": {
            "zip_code": "90012",
            "latitude": 34.0614,
            "longitude": -118.2385,
            "watttime_region_full_name": "Los Angeles Dept of Water & Power",
            "signal_type_used": "co2_moer",
            "location_lookup_status": "success",
        },
        "forecast": forecast,
        "baseline": baseline,
        "optimized": optimized,
        "schedule": schedule,
        "metrics": {
            "baseline_cost": 0.12,
            "optimized_cost": 0.09,
            "cost_savings": 0.03,
            "cost_reduction_pct": 25.0,
            "baseline_carbon_kg": 1.2,
            "optimized_carbon_kg": 1.0,
            "carbon_savings_kg": 0.2,
            "carbon_reduction_pct": 16.7,
        },
    }


def test_optimize_endpoint_returns_frontend_safe_sections(monkeypatch) -> None:
    monkeypatch.setattr("src.api.main.execute_optimization", lambda request: _fake_result())

    response = client.post(
        "/api/v1/optimize",
        json={
            "zip_code": "90012",
            "compute_hours_required": 1,
            "deadline": "2026-03-27T06:00:00",
            "objective": "cost",
            "machine_watts": 1000,
            "include_diagnostics": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert set(payload.keys()) == {
        "input",
        "location",
        "pricing",
        "forecast",
        "summary",
        "metrics",
        "schedule",
        "charts",
        "provenance",
        "diagnostics",
    }
    assert payload["location"]["resolved_region"] == "LDWP"
    assert payload["pricing"]["pricing_source"] == "CAISO"
    assert payload["pricing"]["pricing_market"] == "DAM"
    assert payload["pricing"]["badges"][0]["label"] == "Location Lookup"
    assert payload["metrics"]["optimized_cost"] == 0.09
    assert len(payload["schedule"]["selected_intervals"]) == 2
    assert payload["schedule"]["recommended_window"]["selected_interval_count"] == 2
    assert payload["summary"]["cards"][0]["id"] == "optimized_cost"
    assert "price_timeseries" in payload["charts"]
    assert payload["charts"]["baseline_vs_optimized_comparison"]["rows"][0]["metric"] == "Cost"
    assert payload["diagnostics"]["price_alignment_method"] == "merge_asof_backward"


def test_optimize_preflight_options_returns_cors_headers() -> None:
    response = client.options(
        "/api/v1/optimize",
        headers={
            "Origin": "http://127.0.0.1:5173",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://127.0.0.1:5173"
    assert "POST" in response.headers["access-control-allow-methods"]
    assert "Content-Type" in response.headers["access-control-allow-headers"]


def test_optimize_post_returns_cors_headers_for_browser_origin(monkeypatch) -> None:
    monkeypatch.setattr("src.api.main.execute_optimization", lambda request: _fake_result())

    response = client.post(
        "/api/v1/optimize",
        headers={"Origin": "http://127.0.0.1:5173"},
        json={
            "zip_code": "90012",
            "compute_hours_required": 1,
            "deadline": "2026-03-27T06:00:00",
            "objective": "cost",
            "machine_watts": 1000,
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://127.0.0.1:5173"


def test_cors_configuration_explicitly_allows_browser_post_requirements() -> None:
    assert "POST" in ALLOWED_METHODS
    assert "OPTIONS" in ALLOWED_METHODS
    assert "Content-Type" in ALLOWED_HEADERS
    assert "http://127.0.0.1:5173" in ALLOWED_ORIGINS
    assert VERCEL_ORIGIN in ALLOWED_ORIGINS


def test_vercel_preflight_options_succeeds_for_coverage() -> None:
    response = client.options(
        "/api/v1/coverage",
        headers={
            "Origin": VERCEL_ORIGIN,
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == VERCEL_ORIGIN


def test_vercel_preflight_options_succeeds_for_optimize() -> None:
    response = client.options(
        "/api/v1/optimize",
        headers={
            "Origin": VERCEL_ORIGIN,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == VERCEL_ORIGIN
    assert "POST" in response.headers["access-control-allow-methods"]


def test_vercel_preflight_options_succeeds_for_export() -> None:
    response = client.options(
        "/api/v1/export",
        headers={
            "Origin": VERCEL_ORIGIN,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == VERCEL_ORIGIN
    assert "POST" in response.headers["access-control-allow-methods"]


def test_coverage_endpoint_returns_supported_markets() -> None:
    response = client.get("/api/v1/coverage")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"].startswith("Util currently supports live location-aware pricing")
    assert len(payload["supported_live_markets"]) >= 2
    assert payload["unsupported_behavior"]["label"] == "Fallback pricing"


def test_export_endpoint_returns_artifact_info(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.api.main.build_export_response",
        lambda request: ExportResponse(
            export_dir="c:/dev/util/exports/api/util-test",
            run_id="util-test",
            artifacts=[
                {
                    "filename": "util_run_summary.csv",
                    "display_name": "Run Summary",
                    "artifact_type": "csv",
                    "path": "c:/dev/util/exports/api/util-test/util_run_summary.csv",
                    "reference_path": "c:/dev/util/exports/api/util-test/util_run_summary.csv",
                    "size_bytes": 1234,
                }
            ],
            cloud_upload_enabled=False,
            cloud_message="",
            summary={"artifact_count": 1, "includes_provenance_summary": True, "export_type": "csv_package"},
        ),
    )

    response = client.post(
        "/api/v1/export",
        json={
            "zip_code": "90012",
            "compute_hours_required": 1,
            "deadline": "2026-03-27T06:00:00",
            "objective": "cost",
            "machine_watts": 1000,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == "util-test"
    assert payload["artifacts"][0]["filename"] == "util_run_summary.csv"
    assert payload["artifacts"][0]["artifact_type"] == "csv"
    assert payload["summary"]["includes_provenance_summary"] is True
