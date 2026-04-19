from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from src.analysis.feasibility_features import FeasibilityFeatures


# ---------------------------------------------------------------------------
# Tunable constants — change these to recalibrate without touching formulas
# ---------------------------------------------------------------------------

# Coefficient-of-variation caps for volatility normalisation.
# A CV at or above the cap → component score of 100.
# Electricity price CV of 0.50 means std = half the mean — very volatile.
# Carbon CV of 0.30 is similarly aggressive for grid signal.
_MAX_PRICE_CV: float = 0.50
_MAX_CARBON_CV: float = 0.30

# Timing risk split: how much comes from deadline tightness vs window position.
_TIMING_TIGHTNESS_WEIGHT: float = 0.65
_TIMING_MISMATCH_WEIGHT: float = 0.35

# favorable_window_coverage at or above this → load pressure score = 0.
# At 2.0 you have twice as many favorable slots as you need — minimal friction.
_MAX_FAVORABLE_COVERAGE: float = 2.0

# Neutral fallback used for any component when data is fully missing.
_NEUTRAL_SCORE: float = 50.0

# Bucket boundaries (applied to all three combined scores).
# Scores below LOW → "Low risk / High feasibility"
# Scores above HIGH → "High risk / Low feasibility"
_BUCKET_LOW_THRESHOLD: float = 35.0
_BUCKET_HIGH_THRESHOLD: float = 65.0

# Friction score weights — must sum to 1.0.
_W_FRICTION_GRID_STRESS: float = 0.25
_W_FRICTION_PRICE_VOL: float = 0.20
_W_FRICTION_CARBON_INST: float = 0.10
_W_FRICTION_TIMING: float = 0.30
_W_FRICTION_LOAD: float = 0.15

# Delay risk weights — emphasises timing and market instability; must sum to 1.0.
_W_DELAY_TIMING: float = 0.50
_W_DELAY_GRID: float = 0.20
_W_DELAY_PRICE_VOL: float = 0.20
_W_DELAY_CARBON_INST: float = 0.10


# ---------------------------------------------------------------------------
# Output contract
# ---------------------------------------------------------------------------

Bucket = Literal["Low", "Moderate", "High"]


@dataclass(frozen=True)
class FeasibilityScores:
    """
    Scored output for one opportunity screening run.

    Convention: all component and combined scores are 0–100.
    For COMPONENT scores: higher = more risk / stress.
    For COMBINED scores:
      - friction_score    : higher = more infrastructure friction (bad)
      - feasibility_score : higher = more deployment feasibility (good)
      - delay_risk_score  : higher = higher cost/risk of delaying the run (bad)

    Bucket labels follow the same direction as their parent score.
    """

    # ---- Component scores (0–100, higher = more risk) ------------------- #

    grid_stress_score: float
    """Derived from grid_stress_proxy (mean of high_carbon_share and price_spike_share).
    Captures how congested / dirty the grid looks across the window."""

    price_volatility_risk: float
    """Derived from price coefficient of variation (price_std / price_mean).
    High = prices swing a lot; scheduling can exploit this but it also adds uncertainty."""

    carbon_instability_risk: float
    """Derived from carbon coefficient of variation (carbon_std / carbon_mean).
    High = carbon signal is noisy; clean windows may be short or unpredictable."""

    timing_risk: float
    """Blend of deadline_tightness (65%) and timing_mismatch_score (35%).
    High = deadline is close AND/OR favorable windows fall late in the window."""

    load_pressure_score: float
    """Derived from favorable_window_coverage (available favorable slots / compute hours needed).
    High = not enough cheap-clean slots to cover the workload comfortably."""

    # ---- Combined scores ------------------------------------------------ #

    friction_score: float
    """Weighted sum of the five component scores.
    Weights: grid_stress 25%, price_vol 20%, carbon_inst 10%, timing 30%, load 15%."""

    feasibility_score: float
    """100 − friction_score. Higher = easier to schedule well."""

    delay_risk_score: float
    """Weighted sum emphasising timing (50%) and market signals (30% + 20%).
    Answers: how much does waiting hurt?"""

    # ---- Bucket labels -------------------------------------------------- #

    friction_bucket: Bucket
    """Low / Moderate / High relative to friction_score thresholds."""

    feasibility_bucket: Bucket
    """Low / Moderate / High relative to feasibility_score thresholds.
    Note: bucket direction is inverted vs friction (High feasibility = good)."""

    delay_risk_bucket: Bucket
    """Low / Moderate / High relative to delay_risk_score thresholds."""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def calculate_feasibility_scores(features: FeasibilityFeatures) -> FeasibilityScores:
    """
    Derive all FeasibilityScores from a FeasibilityFeatures instance.

    Every component is independently normalised to 0–100 before weighting.
    Missing data (None features) falls back to ``_NEUTRAL_SCORE`` so partial
    data never crashes scoring — it just produces a less informed result.

    Parameters
    ----------
    features:
        Output of ``extract_feasibility_features()``.

    Returns
    -------
    FeasibilityScores
        All fields are always present.
    """
    grid   = _score_grid_stress(features)
    price  = _score_price_volatility(features)
    carbon = _score_carbon_instability(features)
    timing = _score_timing_risk(features)
    load   = _score_load_pressure(features)

    friction    = _weighted_sum([
        (_W_FRICTION_GRID_STRESS, grid),
        (_W_FRICTION_PRICE_VOL,   price),
        (_W_FRICTION_CARBON_INST, carbon),
        (_W_FRICTION_TIMING,      timing),
        (_W_FRICTION_LOAD,        load),
    ])
    feasibility = _clip(100.0 - friction)

    # Hard infeasibility override: when the workload cannot fit in the available
    # window (deadline_tightness >= 1.0), feasibility is definitionally zero
    # regardless of how benign the grid conditions look.
    if features.deadline_tightness is not None and features.deadline_tightness >= 1.0:
        feasibility = 0.0
        friction    = 100.0

    delay       = _weighted_sum([
        (_W_DELAY_TIMING,       timing),
        (_W_DELAY_GRID,         grid),
        (_W_DELAY_PRICE_VOL,    price),
        (_W_DELAY_CARBON_INST,  carbon),
    ])

    return FeasibilityScores(
        grid_stress_score=grid,
        price_volatility_risk=price,
        carbon_instability_risk=carbon,
        timing_risk=timing,
        load_pressure_score=load,
        friction_score=friction,
        feasibility_score=feasibility,
        delay_risk_score=delay,
        friction_bucket=_bucket(friction),
        feasibility_bucket=_bucket_inverted(feasibility),
        delay_risk_bucket=_bucket(delay),
    )


# ---------------------------------------------------------------------------
# Component scorers
# ---------------------------------------------------------------------------

def _score_grid_stress(f: FeasibilityFeatures) -> float:
    """
    Formula: grid_stress_proxy * 100, clipped to 0–100.

    grid_stress_proxy = mean(high_carbon_share, price_spike_share).
    Both inputs are fractions in [0, 1], so the proxy is in [0, 1].

    Typical value on any distribution: ~0.25 (by percentile construction).
    Elevated values mean the top-quartile intervals dominate the window —
    i.e., most of the window is expensive and dirty at the same time.
    """
    if f.grid_stress_proxy is None:
        return _NEUTRAL_SCORE
    return _clip(f.grid_stress_proxy * 100.0)


def _score_price_volatility(f: FeasibilityFeatures) -> float:
    """
    Formula: (price_std / price_mean) / _MAX_PRICE_CV * 100, clipped to 0–100.

    Uses the coefficient of variation so that absolute price level doesn't
    inflate the score.  A flat $0.25/kWh signal scores 0; a signal where
    std equals half the mean (_MAX_PRICE_CV = 0.50) scores 100.
    """
    if f.price_volatility is None or f.avg_price is None or f.avg_price == 0:
        return _NEUTRAL_SCORE
    cv = f.price_volatility / f.avg_price
    return _clip(cv / _MAX_PRICE_CV * 100.0)


def _score_carbon_instability(f: FeasibilityFeatures) -> float:
    """
    Formula: (carbon_std / carbon_mean) / _MAX_CARBON_CV * 100, clipped to 0–100.

    Same CV approach as price.  Carbon signals typically have lower CV than
    prices, hence the tighter cap (_MAX_CARBON_CV = 0.30).
    """
    if f.carbon_volatility is None or f.avg_carbon is None or f.avg_carbon == 0:
        return _NEUTRAL_SCORE
    cv = f.carbon_volatility / f.avg_carbon
    return _clip(cv / _MAX_CARBON_CV * 100.0)


def _score_timing_risk(f: FeasibilityFeatures) -> float:
    """
    Formula: 0.65 * tightness_component + 0.35 * mismatch_component.

    tightness_component:
      deadline_tightness * 100, capped at 100.
      > 100 means infeasible (more compute needed than window allows) → score 100.

    mismatch_component:
      timing_mismatch_score * 100.
      Measures how late in the window the favorable slots appear.
      Falls back to _NEUTRAL_SCORE when unavailable.

    Both components are independently clipped before blending.
    """
    tightness = f.deadline_tightness
    if tightness is None:
        tightness_component = _NEUTRAL_SCORE
    else:
        # Clamp at 100 — beyond 1.0 is already the worst case (infeasible).
        tightness_component = _clip(tightness * 100.0, lo=0.0, hi=100.0)

    mismatch = f.timing_mismatch_score
    mismatch_component = _NEUTRAL_SCORE if mismatch is None else _clip(mismatch * 100.0)

    return _clip(
        _TIMING_TIGHTNESS_WEIGHT * tightness_component
        + _TIMING_MISMATCH_WEIGHT * mismatch_component
    )


def _score_load_pressure(f: FeasibilityFeatures) -> float:
    """
    Formula: (1 - min(favorable_window_coverage, _MAX_FAVORABLE_COVERAGE)
                  / _MAX_FAVORABLE_COVERAGE) * 100, clipped to 0–100.

    Interpretation:
      favorable_window_coverage >= _MAX_FAVORABLE_COVERAGE  → score 0   (abundant slots)
      favorable_window_coverage == 1.0                      → score 50  (just enough slots)
      favorable_window_coverage == 0                        → score 100 (no favorable slots)

    Falls back to a bucket-derived score when favorable_window_coverage is None.
    """
    coverage = f.favorable_window_coverage
    if coverage is None:
        return _bucket_to_load_score(f.relative_load_bucket)

    capped = min(coverage, _MAX_FAVORABLE_COVERAGE)
    return _clip((1.0 - capped / _MAX_FAVORABLE_COVERAGE) * 100.0)


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def _clip(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    """Hard-clip a value into [lo, hi]."""
    return max(lo, min(hi, value))


def _weighted_sum(pairs: list[tuple[float, float]]) -> float:
    """
    Compute a weighted sum of (weight, score) pairs and clip to [0, 100].
    Weights do not need to be pre-validated — the result is always clipped.
    """
    total = sum(w * s for w, s in pairs)
    return _clip(total)


def _bucket(score: float) -> Bucket:
    """
    Map a score to a risk bucket.  Higher score = more risk.
      score < _BUCKET_LOW_THRESHOLD  → "Low"
      score > _BUCKET_HIGH_THRESHOLD → "High"
      otherwise                      → "Moderate"
    """
    if score >= _BUCKET_HIGH_THRESHOLD:
        return "High"
    if score <= _BUCKET_LOW_THRESHOLD:
        return "Low"
    return "Moderate"


def _bucket_inverted(score: float) -> Bucket:
    """
    Map a feasibility score to a bucket.  Higher score = better.
    Inverts the threshold direction vs _bucket() so "High" means good.
      score > _BUCKET_HIGH_THRESHOLD → "High"   (great feasibility)
      score < _BUCKET_LOW_THRESHOLD  → "Low"    (poor feasibility)
      otherwise                      → "Moderate"
    """
    if score >= _BUCKET_HIGH_THRESHOLD:
        return "High"
    if score <= _BUCKET_LOW_THRESHOLD:
        return "Low"
    return "Moderate"


def _bucket_to_load_score(bucket: str) -> float:
    """
    Fallback when favorable_window_coverage is unavailable.
    Maps relative_load_bucket to a representative load pressure score.
    """
    return {"light": 20.0, "medium": 50.0, "heavy": 80.0}.get(bucket, _NEUTRAL_SCORE)
