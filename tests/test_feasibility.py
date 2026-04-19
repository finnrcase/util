"""
Tests for the Opportunity Screening / Feasibility Analysis layer.

Covers:
  - Feature extraction (feasibility_features.py)
  - Scoring (feasibility_scoring.py)
  - Explanation engine (feasibility_explanations.py)
  - End-to-end pipeline via run_feasibility_from_pipeline_result

Scenarios tested:
  1. Low-risk   — long horizon, flat cheap+clean grid, light workload
  2. Moderate   — moderate deadline pressure, some price volatility
  3. High-risk  — tight deadline, volatile prices, no favorable windows
  4. Edge: missing price column
  5. Edge: missing carbon column
  6. Edge: compute hours > available intervals (infeasible)
  7. Edge: very small machine wattage (1W)
  8. Edge: very large machine wattage (500 kW)
  9. Edge: very short horizon (3-hour window)
  10. Edge: long horizon / fallback (72-hour window, stable grid)
"""

from __future__ import annotations

from pathlib import Path
import sys
from datetime import datetime, timedelta

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.analysis.feasibility_features import extract_feasibility_features
from src.analysis.feasibility_scoring import calculate_feasibility_scores
from src.analysis.feasibility_explanations import (
    generate_feasibility_explanation,
    build_recommendation,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_TIME = datetime(2026, 4, 18, 0, 0, 0)


def _make_forecast(
    n_hours: int = 24,
    price_pattern: list[float] | None = None,
    carbon_pattern: list[float] | None = None,
    include_price: bool = True,
    include_carbon: bool = True,
    start: datetime = _BASE_TIME,
) -> pd.DataFrame:
    """
    Build a minimal forecast DataFrame that mirrors what get_forecast_table()
    returns: columns timestamp, price_per_kwh, carbon_g_per_kwh.

    If a pattern is shorter than n_hours it is tiled; if longer it is truncated.
    Pass include_price=False / include_carbon=False to simulate missing columns.
    """
    timestamps = pd.date_range(start, periods=n_hours, freq="h")
    df = pd.DataFrame({"timestamp": timestamps})

    if include_price:
        if price_pattern is None:
            price_pattern = [0.10] * n_hours
        tiled = (price_pattern * ((n_hours // len(price_pattern)) + 1))[:n_hours]
        df["price_per_kwh"] = tiled

    if include_carbon:
        if carbon_pattern is None:
            carbon_pattern = [300.0] * n_hours
        tiled = (carbon_pattern * ((n_hours // len(carbon_pattern)) + 1))[:n_hours]
        df["carbon_g_per_kwh"] = tiled

    return df


def _deadline(hours_from_start: int, start: datetime = _BASE_TIME) -> str:
    return (start + timedelta(hours=hours_from_start)).isoformat()


def _run(
    forecast_df: pd.DataFrame,
    compute_hours: int,
    deadline_hours: int,
    machine_watts: int = 1000,
    start: datetime = _BASE_TIME,
):
    """Run the full feature → score → explanation → recommendation pipeline."""
    deadline = _deadline(deadline_hours, start)
    features = extract_feasibility_features(forecast_df, compute_hours, deadline, machine_watts)
    scores = calculate_feasibility_scores(features)
    explanation = generate_feasibility_explanation(features, scores)
    recommendation = build_recommendation(features, scores, explanation)
    return features, scores, explanation, recommendation


# ===========================================================================
# Scenario 1 — Low-risk
# ===========================================================================

class TestLowRisk:
    """
    24-hour window, 4-hour workload, flat cheap + clean grid.
    Expected: high feasibility, low friction, Prioritize or Promising.
    """

    def setup_method(self):
        # Flat, low price and carbon — no volatility, all intervals favorable.
        forecast = _make_forecast(
            n_hours=24,
            price_pattern=[0.08],
            carbon_pattern=[180.0],
        )
        self.f, self.s, self.e, self.r = _run(forecast, compute_hours=4, deadline_hours=24)

    def test_feasibility_score_is_high(self):
        assert self.s.feasibility_score >= 60, (
            f"Expected high feasibility for low-risk scenario, got {self.s.feasibility_score:.1f}"
        )

    def test_friction_score_is_low(self):
        assert self.s.friction_score <= 45, (
            f"Expected low friction, got {self.s.friction_score:.1f}"
        )

    def test_timing_risk_is_low(self):
        # 4h required / 24h available = 16.7% tightness → low timing risk.
        assert self.s.timing_risk <= 50, f"Timing risk too high: {self.s.timing_risk:.1f}"

    def test_deadline_tightness_is_low(self):
        assert self.f.deadline_tightness is not None
        assert self.f.deadline_tightness <= 0.25

    def test_recommendation_category_is_positive(self):
        assert self.r.category in ("Prioritize", "Promising but monitor"), (
            f"Expected positive recommendation for low-risk scenario, got {self.r.category!r}"
        )

    def test_overall_label_is_strong_or_moderate(self):
        assert self.e.overall_label in ("Strong", "Moderate"), (
            f"Expected Strong or Moderate, got {self.e.overall_label!r}"
        )

    def test_load_pressure_reflects_low_cost(self):
        # avg_price=0.08, 4h, 1kW → $0.32 estimated cost.
        assert self.f.load_pressure is not None
        assert self.f.load_pressure < 1.0, f"Load pressure unexpectedly high: {self.f.load_pressure}"

    def test_no_risk_drivers_dominate(self):
        risk_drivers = [d for d in self.e.drivers if d.direction == "risk"]
        # In a low-risk scenario there may be 0 or at most 1 mild risk driver.
        for d in risk_drivers:
            assert d.severity < 70, (
                f"Unexpected high-severity risk driver in low-risk scenario: {d.key} ({d.severity})"
            )


# ===========================================================================
# Scenario 2 — Moderate-risk
# ===========================================================================

class TestModerateRisk:
    """
    24-hour window, 12-hour workload, moderate price volatility, some carbon spikes.
    Expected: moderate feasibility, moderate friction, Promising or Caution.
    """

    def setup_method(self):
        # Price alternates low/high — moderate volatility.
        price = [0.06, 0.06, 0.14, 0.14, 0.06, 0.06, 0.18, 0.18,
                 0.06, 0.07, 0.15, 0.16, 0.07, 0.07, 0.17, 0.19,
                 0.08, 0.08, 0.13, 0.14, 0.08, 0.09, 0.16, 0.16]
        carbon = [280, 290, 380, 390, 260, 270, 420, 430,
                  250, 260, 370, 380, 240, 250, 410, 420,
                  270, 280, 360, 370, 265, 275, 400, 410]
        forecast = _make_forecast(n_hours=24, price_pattern=price, carbon_pattern=carbon)
        self.f, self.s, self.e, self.r = _run(forecast, compute_hours=12, deadline_hours=24)

    def test_feasibility_score_is_moderate(self):
        # 12h required / 24h available → 50% tightness → moderate.
        assert 20 <= self.s.feasibility_score <= 75, (
            f"Expected moderate feasibility, got {self.s.feasibility_score:.1f}"
        )

    def test_deadline_tightness_is_moderate(self):
        assert self.f.deadline_tightness is not None
        assert 0.40 <= self.f.deadline_tightness <= 0.70

    def test_price_volatility_risk_is_elevated(self):
        # CV of the zigzag price series should register above neutral.
        assert self.s.price_volatility_risk > 30, (
            f"Expected elevated price vol risk, got {self.s.price_volatility_risk:.1f}"
        )

    def test_recommendation_is_promising_or_caution(self):
        assert self.r.category in ("Promising but monitor", "Caution"), (
            f"Got {self.r.category!r}"
        )

    def test_drivers_are_present(self):
        assert len(self.e.drivers) >= 2

    def test_interpretation_is_non_empty(self):
        assert len(self.e.interpretation) > 20

    def test_recommendation_body_is_non_empty(self):
        assert len(self.r.body) > 40


# ===========================================================================
# Scenario 3 — High-risk
# ===========================================================================

class TestHighRisk:
    """
    10-hour window, 8-hour workload, spiking prices, dirty grid.
    Expected: low feasibility, high friction, Caution or Deprioritize.
    """

    def setup_method(self):
        price  = [0.08, 0.22, 0.25, 0.08, 0.24, 0.26, 0.08, 0.27, 0.25, 0.09]
        carbon = [400, 500, 520, 410, 510, 530, 420, 540, 510, 430]
        forecast = _make_forecast(n_hours=10, price_pattern=price, carbon_pattern=carbon)
        self.f, self.s, self.e, self.r = _run(forecast, compute_hours=8, deadline_hours=10)

    def test_feasibility_score_is_low(self):
        assert self.s.feasibility_score <= 55, (
            f"Expected low feasibility for high-risk scenario, got {self.s.feasibility_score:.1f}"
        )

    def test_friction_score_is_high(self):
        assert self.s.friction_score >= 45, (
            f"Expected high friction, got {self.s.friction_score:.1f}"
        )

    def test_timing_risk_is_high(self):
        # 8h required / 10h available = 80% tightness.
        assert self.s.timing_risk >= 50, f"Timing risk too low: {self.s.timing_risk:.1f}"

    def test_deadline_tightness_is_high(self):
        assert self.f.deadline_tightness is not None
        assert self.f.deadline_tightness >= 0.70

    def test_recommendation_is_caution_or_deprioritize(self):
        assert self.r.category in ("Caution", "Deprioritize"), (
            f"Got {self.r.category!r}"
        )

    def test_tight_deadline_driver_fires(self):
        driver_keys = [d.key for d in self.e.drivers]
        assert any(k in driver_keys for k in ("tight_deadline", "high_runtime_density")), (
            f"Expected timing driver, got: {driver_keys}"
        )

    def test_load_pressure_score_is_elevated(self):
        # Few favorable windows → high load pressure.
        assert self.s.load_pressure_score >= 40, (
            f"Expected elevated load pressure, got {self.s.load_pressure_score:.1f}"
        )


# ===========================================================================
# Edge case 4 — Missing price data
# ===========================================================================

class TestMissingPrice:
    """
    Forecast has no price_per_kwh column.
    All price-derived features must be None; carbon features must still work.
    Scoring must not crash; recommendation must still be produced.
    """

    def setup_method(self):
        forecast = _make_forecast(n_hours=24, include_price=False, carbon_pattern=[300.0])
        self.f, self.s, self.e, self.r = _run(forecast, compute_hours=4, deadline_hours=24)

    def test_price_features_are_none(self):
        assert self.f.avg_price is None
        assert self.f.peak_price is None
        assert self.f.price_volatility is None
        assert self.f.cheap_window_share is None
        assert self.f.price_spike_share is None

    def test_carbon_features_are_still_computed(self):
        assert self.f.avg_carbon is not None
        assert self.f.carbon_volatility is not None

    def test_load_pressure_is_none_without_price(self):
        # load_pressure = energy * avg_price; no avg_price → None.
        assert self.f.load_pressure is None

    def test_scores_do_not_crash(self):
        # price_volatility_risk and grid_stress_proxy should fall back to neutral.
        assert 0.0 <= self.s.price_volatility_risk <= 100.0
        assert 0.0 <= self.s.feasibility_score <= 100.0

    def test_recommendation_is_produced(self):
        assert self.r.category in ("Prioritize", "Promising but monitor", "Caution", "Deprioritize")
        assert len(self.r.body) > 10

    def test_no_price_claims_in_drivers(self):
        # Drivers must not reference price values that don't exist.
        for d in self.e.drivers:
            if d.key in ("high_price_volatility", "frequent_peak_pricing"):
                # These should not fire when price data is absent.
                assert False, f"Price driver {d.key!r} fired with no price data"


# ===========================================================================
# Edge case 5 — Missing carbon data
# ===========================================================================

class TestMissingCarbon:
    """
    Forecast has no carbon_g_per_kwh column.
    All carbon-derived features must be None; price features must work.
    """

    def setup_method(self):
        forecast = _make_forecast(n_hours=24, include_carbon=False, price_pattern=[0.10])
        self.f, self.s, self.e, self.r = _run(forecast, compute_hours=4, deadline_hours=24)

    def test_carbon_features_are_none(self):
        assert self.f.avg_carbon is None
        assert self.f.carbon_volatility is None
        assert self.f.clean_window_share is None
        assert self.f.high_carbon_share is None

    def test_price_features_are_computed(self):
        assert self.f.avg_price is not None

    def test_scores_do_not_crash(self):
        assert 0.0 <= self.s.carbon_instability_risk <= 100.0
        assert 0.0 <= self.s.feasibility_score <= 100.0

    def test_recommendation_is_produced(self):
        assert len(self.r.action) > 5

    def test_no_carbon_claims_in_drivers(self):
        for d in self.e.drivers:
            if d.key in ("unstable_carbon_profile",):
                assert False, f"Carbon driver fired with no carbon data"


# ===========================================================================
# Edge case 6 — Compute hours > available intervals (infeasible)
# ===========================================================================

class TestInfeasible:
    """
    Only 6 forecast intervals before the deadline but workload needs 10h.
    Expected: overall_label="Infeasible", category="Deprioritize".
    """

    def setup_method(self):
        forecast = _make_forecast(n_hours=24, price_pattern=[0.10], carbon_pattern=[300.0])
        # Deadline is only 6h out — fewer than the 10h needed.
        self.f, self.s, self.e, self.r = _run(forecast, compute_hours=10, deadline_hours=6)

    def test_deadline_tightness_exceeds_one(self):
        assert self.f.deadline_tightness is not None
        assert self.f.deadline_tightness >= 1.0, (
            f"Expected tightness >= 1.0 for infeasible case, got {self.f.deadline_tightness:.2f}"
        )

    def test_overall_label_is_infeasible(self):
        assert self.e.overall_label == "Infeasible", (
            f"Expected Infeasible, got {self.e.overall_label!r}"
        )

    def test_recommendation_is_deprioritize(self):
        assert self.r.category == "Deprioritize", (
            f"Expected Deprioritize, got {self.r.category!r}"
        )

    def test_recommendation_body_mentions_deadline_or_compute(self):
        body_lower = self.r.body.lower()
        assert any(kw in body_lower for kw in ("deadline", "compute", "hours", "window", "cannot")), (
            f"Infeasible body does not reference the constraint: {self.r.body!r}"
        )

    def test_feasibility_score_is_low(self):
        # Infeasible → friction should be very high → feasibility low.
        assert self.s.feasibility_score <= 30, (
            f"Expected low feasibility score, got {self.s.feasibility_score:.1f}"
        )


# ===========================================================================
# Edge case 7 — Very small machine wattage (1W)
# ===========================================================================

class TestTinyMachine:
    """
    1W machine: load_energy_required_kwh should be tiny,
    load_pressure in USD should be near zero.
    Scores must not crash or produce nonsense.
    """

    def setup_method(self):
        forecast = _make_forecast(n_hours=24, price_pattern=[0.10], carbon_pattern=[300.0])
        self.f, self.s, self.e, self.r = _run(
            forecast, compute_hours=4, deadline_hours=24, machine_watts=1
        )

    def test_machine_kw_is_tiny(self):
        assert self.f.machine_kw == pytest.approx(0.001)

    def test_load_energy_is_tiny(self):
        # 0.001 kW * 4h = 0.004 kWh
        assert self.f.load_energy_required_kwh == pytest.approx(0.004)

    def test_load_pressure_is_near_zero(self):
        assert self.f.load_pressure is not None
        assert self.f.load_pressure < 0.01

    def test_large_load_driver_does_not_fire(self):
        # 1W is trivially light — "large_load_requirement" must not fire.
        driver_keys = [d.key for d in self.e.drivers]
        assert "large_load_requirement" not in driver_keys

    def test_relative_load_bucket_is_not_heavy(self):
        # Load bucket is driven by deadline_tightness, not wattage.
        # 4h/24h = 0.17 → light bucket.
        assert self.f.relative_load_bucket == "light"

    def test_scores_are_sane(self):
        assert 0.0 <= self.s.feasibility_score <= 100.0
        assert 0.0 <= self.s.load_pressure_score <= 100.0


# ===========================================================================
# Edge case 8 — Very large machine wattage (500 kW)
# ===========================================================================

class TestHugeMachine:
    """
    500,000W machine: load_energy_required_kwh is very large.
    load_pressure in USD should reflect that.
    Scores must not overflow or crash.
    """

    def setup_method(self):
        forecast = _make_forecast(n_hours=24, price_pattern=[0.10], carbon_pattern=[300.0])
        self.f, self.s, self.e, self.r = _run(
            forecast, compute_hours=4, deadline_hours=24, machine_watts=500_000
        )

    def test_machine_kw_is_large(self):
        assert self.f.machine_kw == pytest.approx(500.0)

    def test_load_energy_is_large(self):
        # 500 kW * 4h = 2000 kWh
        assert self.f.load_energy_required_kwh == pytest.approx(2000.0)

    def test_load_pressure_reflects_scale(self):
        assert self.f.load_pressure is not None
        # 2000 kWh * $0.10/kWh = $200
        assert self.f.load_pressure == pytest.approx(200.0, rel=0.01)

    def test_scores_do_not_overflow(self):
        assert 0.0 <= self.s.feasibility_score <= 100.0
        assert 0.0 <= self.s.friction_score <= 100.0
        assert 0.0 <= self.s.load_pressure_score <= 100.0

    def test_recommendation_is_produced(self):
        assert self.r.category in ("Prioritize", "Promising but monitor", "Caution", "Deprioritize")


# ===========================================================================
# Edge case 9 — Very short horizon (3-hour window, 2h workload)
# ===========================================================================

class TestShortHorizon:
    """
    Only 3 hours of forecast. Workload needs 2h.
    Percentile-based features require >= 2 data points.
    Must not crash; deadline_tightness must be valid.
    """

    def setup_method(self):
        price  = [0.08, 0.12, 0.09]
        carbon = [280, 310, 295]
        forecast = _make_forecast(n_hours=3, price_pattern=price, carbon_pattern=carbon)
        self.f, self.s, self.e, self.r = _run(forecast, compute_hours=2, deadline_hours=3)

    def test_features_do_not_crash(self):
        assert self.f.compute_hours_required == 2

    def test_deadline_tightness_is_valid(self):
        assert self.f.deadline_tightness is not None
        # 2h required / 3h available ≈ 0.67
        assert 0.60 <= self.f.deadline_tightness <= 0.75

    def test_scores_are_in_range(self):
        assert 0.0 <= self.s.feasibility_score <= 100.0
        assert 0.0 <= self.s.timing_risk <= 100.0

    def test_recommendation_is_produced(self):
        assert len(self.r.headline) > 0
        assert len(self.r.action) > 0

    def test_label_is_valid(self):
        assert self.e.overall_label in ("Strong", "Moderate", "Marginal", "Infeasible")


# ===========================================================================
# Edge case 10 — Long horizon / fallback (72-hour window, stable grid)
# ===========================================================================

class TestLongHorizon:
    """
    72-hour window, 4-hour workload, perfectly stable grid.
    Expected: near-maximum feasibility, minimum timing risk,
    Prioritize recommendation.
    """

    def setup_method(self):
        forecast = _make_forecast(
            n_hours=72,
            price_pattern=[0.09],
            carbon_pattern=[200.0],
        )
        self.f, self.s, self.e, self.r = _run(forecast, compute_hours=4, deadline_hours=72)

    def test_deadline_tightness_is_very_low(self):
        assert self.f.deadline_tightness is not None
        # 4/72 ≈ 0.056
        assert self.f.deadline_tightness < 0.10

    def test_timing_risk_is_very_low(self):
        assert self.s.timing_risk < 40

    def test_feasibility_score_is_high(self):
        assert self.s.feasibility_score >= 55

    def test_load_bucket_is_light(self):
        assert self.f.relative_load_bucket == "light"

    def test_favorable_window_coverage_exceeds_requirement(self):
        # Flat cheap+clean grid → every interval is favorable → coverage >> 1.
        assert self.f.favorable_window_coverage is not None
        assert self.f.favorable_window_coverage >= 1.0

    def test_recommendation_is_prioritize_or_promising(self):
        assert self.r.category in ("Prioritize", "Promising but monitor"), (
            f"Expected positive recommendation for long-horizon stable grid, got {self.r.category!r}"
        )

    def test_volatility_is_effectively_zero(self):
        # Flat price series → std = 0 → price_volatility_risk should be 0 or near neutral.
        # Note: with population std (ddof=0) on a uniform series, std=0 → CV=0 → risk=0.
        assert self.f.price_volatility is not None
        assert self.f.price_volatility == pytest.approx(0.0, abs=1e-6)


# ===========================================================================
# Additional: score range invariants across all scenarios
# ===========================================================================

ALL_SCENARIOS = [
    # (n_hours, price, carbon, compute, deadline_hours, watts)
    (24, [0.08], [180.0], 4, 24, 1000),          # low risk
    (24, None, None, 12, 24, 1000),               # moderate (default series)
    (10, [0.08, 0.22, 0.25, 0.08], [400, 500], 8, 10, 1000),  # high risk
    (24, None, [300.0], 4, 24, 1000),             # missing price
    (24, [0.10], None, 4, 24, 1000),              # missing carbon
    (24, [0.10], [300.0], 4, 3, 1000),            # short deadline (infeasible)
    (3, [0.08, 0.12, 0.09], [280, 310, 295], 2, 3, 1000),     # short horizon
    (72, [0.09], [200.0], 4, 72, 1000),           # long horizon
    (24, [0.10], [300.0], 4, 24, 1),              # tiny machine
    (24, [0.10], [300.0], 4, 24, 500_000),        # huge machine
]


@pytest.mark.parametrize("params", ALL_SCENARIOS)
def test_all_scores_are_in_valid_range(params):
    """All combined and component scores must always be in [0, 100]."""
    n_hours, price, carbon, compute, deadline_h, watts = params
    include_price = price is not None
    include_carbon = carbon is not None

    forecast = _make_forecast(
        n_hours=n_hours,
        price_pattern=price,
        carbon_pattern=carbon,
        include_price=include_price,
        include_carbon=include_carbon,
    )
    _, scores, _, _ = _run(forecast, compute_hours=compute, deadline_hours=deadline_h, machine_watts=watts)

    for field_name in (
        "grid_stress_score", "price_volatility_risk", "carbon_instability_risk",
        "timing_risk", "load_pressure_score",
        "friction_score", "feasibility_score", "delay_risk_score",
    ):
        value = getattr(scores, field_name)
        assert 0.0 <= value <= 100.0, (
            f"{field_name}={value:.2f} is outside [0, 100] for params={params}"
        )


@pytest.mark.parametrize("params", ALL_SCENARIOS)
def test_feasibility_plus_friction_near_100(params):
    """feasibility_score + friction_score should always sum to 100."""
    n_hours, price, carbon, compute, deadline_h, watts = params
    forecast = _make_forecast(
        n_hours=n_hours,
        price_pattern=price,
        carbon_pattern=carbon,
        include_price=(price is not None),
        include_carbon=(carbon is not None),
    )
    _, scores, _, _ = _run(forecast, compute_hours=compute, deadline_hours=deadline_h, machine_watts=watts)
    total = scores.feasibility_score + scores.friction_score
    assert abs(total - 100.0) < 0.02, (
        f"feasibility + friction = {total:.2f}, expected 100. params={params}"
    )


@pytest.mark.parametrize("params", ALL_SCENARIOS)
def test_recommendation_fields_are_non_empty(params):
    """Every scenario must produce a complete recommendation with no empty strings."""
    n_hours, price, carbon, compute, deadline_h, watts = params
    forecast = _make_forecast(
        n_hours=n_hours,
        price_pattern=price,
        carbon_pattern=carbon,
        include_price=(price is not None),
        include_carbon=(carbon is not None),
    )
    _, _, _, rec = _run(forecast, compute_hours=compute, deadline_hours=deadline_h, machine_watts=watts)
    assert rec.category in ("Prioritize", "Promising but monitor", "Caution", "Deprioritize")
    assert len(rec.headline) > 5, f"Empty headline for params={params}"
    assert len(rec.body) > 20, f"Empty body for params={params}"
    assert len(rec.action) > 10, f"Empty action for params={params}"
