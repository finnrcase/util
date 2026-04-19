from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from src.analysis.feasibility_features import FeasibilityFeatures
from src.analysis.feasibility_scoring import FeasibilityScores


# ---------------------------------------------------------------------------
# Tunable thresholds — all driver trigger conditions live here
# ---------------------------------------------------------------------------

# Timing drivers
_TIGHT_DEADLINE_TIGHTNESS_THRESHOLD: float = 0.55
"""deadline_tightness at or above this → tight_deadline driver fires."""

_HIGH_RUNTIME_DENSITY_THRESHOLD: float = 0.75
"""runtime_density at or above this → high_runtime_density driver fires."""

_TIMING_MISMATCH_THRESHOLD: float = 0.55
"""timing_mismatch_score at or above this → timing_constraint_mismatch fires."""

# Price / market drivers
_PEAK_PRICE_FREQ_THRESHOLD: float = 0.12
"""peak_price_frequency at or above this → frequent_peak_pricing fires.
12% means more than 1-in-8 intervals are in the top decile — above the 10% baseline."""

_PRICE_VOL_RISK_THRESHOLD: float = 40.0
"""price_volatility_risk score at or above this → high_price_volatility fires."""

_GRID_STRESS_THRESHOLD: float = 55.0
"""grid_stress_score at or above this → grid_stress driver fires."""

# Carbon drivers
_CARBON_INSTABILITY_THRESHOLD: float = 40.0
"""carbon_instability_risk score at or above this → unstable_carbon_profile fires."""

# Favorable window / load drivers
_LIMITED_COVERAGE_THRESHOLD: float = 0.75
"""favorable_window_coverage below this → limited_favorable_windows fires."""

# Overall label thresholds (applied to feasibility_score)
_LABEL_STRONG: float = 70.0
_LABEL_MODERATE: float = 45.0
_LABEL_MARGINAL: float = 20.0

# Driver selection bounds
_MIN_DRIVERS: int = 2
_MAX_DRIVERS: int = 4


# ---------------------------------------------------------------------------
# Output shapes
# ---------------------------------------------------------------------------

OverallLabel = Literal["Strong", "Moderate", "Marginal", "Infeasible"]
DriverDirection = Literal["risk", "opportunity"]


@dataclass(frozen=True)
class FeasibilityDriver:
    """One ranked explanation item — structured for API consumption."""

    key: str
    """Machine-readable identifier, e.g. ``"tight_deadline"``.
    Stable across runs; safe to use as a UI key or filter."""

    label: str
    """Short human-readable label, e.g. ``"Tight deadline"``.
    Suitable for card titles or badge text."""

    rank: int
    """1-indexed severity rank within the returned driver list. 1 = worst."""

    severity: float
    """0–100. Used internally for ranking; expose to UI if you want a severity bar."""

    direction: DriverDirection
    """``"risk"`` = hurts scheduling feasibility; ``"opportunity"`` = helps."""

    detail: str
    """One concrete sentence grounded in actual metric values.
    No hedging language. No inferred causes."""


@dataclass(frozen=True)
class FeasibilityExplanation:
    """Full explanation output — structured for API and UI consumption."""

    overall_label: OverallLabel
    """Top-level label: ``"Strong"`` | ``"Moderate"`` | ``"Marginal"`` | ``"Infeasible"``."""

    drivers: list[FeasibilityDriver]
    """Ranked list of the 2–4 most significant drivers.
    Always at least 2 entries when data is available; may be empty for fully null inputs."""

    interpretation: str
    """1–2 sentence synthesis of what is driving the result.
    References specific driver conditions — not a generic template."""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_feasibility_explanation(
    features: FeasibilityFeatures,
    scores: FeasibilityScores,
) -> FeasibilityExplanation:
    """
    Produce a ranked driver list and interpretation from scored features.

    Fully deterministic — no LLM, no randomness.  Every detail sentence
    references a specific numeric value from ``features`` or ``scores``.

    Parameters
    ----------
    features:
        Output of ``extract_feasibility_features()``.
    scores:
        Output of ``calculate_feasibility_scores()``.

    Returns
    -------
    FeasibilityExplanation
        Always complete.  ``drivers`` may be empty only when all feature
        fields are None (fully missing data).
    """
    label = _overall_label(features, scores)
    candidates = _evaluate_all_drivers(features, scores)
    drivers = _rank_and_select(candidates)
    interpretation = _build_interpretation(label, drivers, features, scores)

    return FeasibilityExplanation(
        overall_label=label,
        drivers=drivers,
        interpretation=interpretation,
    )


# ---------------------------------------------------------------------------
# Overall label
# ---------------------------------------------------------------------------

def _overall_label(features: FeasibilityFeatures, scores: FeasibilityScores) -> OverallLabel:
    """
    Derive the top-level label from feasibility_score and hard infeasibility check.

    Infeasible takes priority when:
      - deadline_tightness >= 1.0 (compute hours exceed available intervals), OR
      - deadline_tightness is None AND hours_until_deadline is None
        (deadline cannot be resolved at all).

    Thresholds (tune via _LABEL_* constants):
      >= 70 → "Strong"
      >= 45 → "Moderate"
      >= 20 → "Marginal"
      <  20 → "Infeasible"
    """
    tightness = features.deadline_tightness
    if tightness is not None and tightness >= 1.0:
        return "Infeasible"
    if tightness is None and features.hours_until_deadline is None:
        return "Infeasible"

    s = scores.feasibility_score
    if s >= _LABEL_STRONG:
        return "Strong"
    if s >= _LABEL_MODERATE:
        return "Moderate"
    if s >= _LABEL_MARGINAL:
        return "Marginal"
    return "Infeasible"


# ---------------------------------------------------------------------------
# Driver evaluation — one function per driver
# ---------------------------------------------------------------------------

# Each evaluator returns (key, label, severity, direction, detail) or None.
# severity=None means the condition is not triggered; the driver is excluded.
_CandidateTuple = tuple[str, str, float, DriverDirection, str] | None


def _eval_tight_deadline(f: FeasibilityFeatures, s: FeasibilityScores) -> _CandidateTuple:
    tightness = f.deadline_tightness
    if tightness is None or tightness < _TIGHT_DEADLINE_TIGHTNESS_THRESHOLD:
        return None
    severity = min(tightness * 100.0, 100.0)
    hours = f.hours_until_deadline
    required = f.compute_hours_required
    if hours is not None:
        detail = (
            f"The workload requires {required}h of compute but the deadline allows "
            f"only {hours:.1f}h of scheduling window, leaving {tightness:.0%} interval density."
        )
    else:
        detail = (
            f"The workload requires {required}h but deadline_tightness is {tightness:.2f}, "
            f"meaning available intervals barely exceed what the workload needs."
        )
    return ("tight_deadline", "Tight deadline", severity, "risk", detail)


def _eval_high_runtime_density(f: FeasibilityFeatures, s: FeasibilityScores) -> _CandidateTuple:
    density = f.runtime_density
    if density is None or density < _HIGH_RUNTIME_DENSITY_THRESHOLD:
        return None
    # Suppress if tight_deadline already fires at similar severity — avoid near-duplicate.
    tightness = f.deadline_tightness or 0.0
    if tightness >= _TIGHT_DEADLINE_TIGHTNESS_THRESHOLD and abs(density - tightness) < 0.10:
        return None
    severity = min(density * 100.0, 100.0)
    hours = f.hours_until_deadline
    required = f.compute_hours_required
    if hours is not None:
        detail = (
            f"Runtime density is {density:.0%}: {required}h of compute against "
            f"{hours:.1f}h until the deadline, leaving minimal room to avoid peak windows."
        )
    else:
        detail = (
            f"Runtime density is {density:.0%} — the workload occupies a large share "
            f"of the available scheduling horizon."
        )
    return ("high_runtime_density", "High runtime density", severity, "risk", detail)


def _eval_timing_mismatch(f: FeasibilityFeatures, s: FeasibilityScores) -> _CandidateTuple:
    mismatch = f.timing_mismatch_score
    if mismatch is None or mismatch < _TIMING_MISMATCH_THRESHOLD:
        return None
    severity = mismatch * 100.0
    detail = (
        f"Favorable intervals are concentrated in the later {mismatch:.0%} of the "
        f"scheduling window — capturing them requires waiting close to the deadline."
    )
    return ("timing_constraint_mismatch", "Timing constraint mismatch", severity, "risk", detail)


def _eval_frequent_peak_pricing(f: FeasibilityFeatures, s: FeasibilityScores) -> _CandidateTuple:
    freq = f.peak_price_frequency
    if freq is None or freq < _PEAK_PRICE_FREQ_THRESHOLD:
        return None
    # Normalise: 10% baseline → score 0; 25%+ → score 100.
    severity = min(max((freq - 0.10) / 0.15, 0.0) * 100.0, 100.0)
    peak = f.peak_price
    avg = f.avg_price
    if peak is not None and avg is not None:
        detail = (
            f"{freq:.0%} of forecast intervals are in the top price decile "
            f"(peak ${peak:.4f}/kWh vs avg ${avg:.4f}/kWh) — high-cost intervals are frequent."
        )
    else:
        detail = (
            f"{freq:.0%} of forecast intervals are in the top price decile, "
            f"making it difficult to consistently avoid peak-price windows."
        )
    return ("frequent_peak_pricing", "Frequent peak pricing", severity, "risk", detail)


def _eval_high_price_volatility(f: FeasibilityFeatures, s: FeasibilityScores) -> _CandidateTuple:
    if f.avg_price is None or f.price_volatility is None:
        return None
    if s.price_volatility_risk < _PRICE_VOL_RISK_THRESHOLD:
        return None
    # Suppress if frequent_peak_pricing already fires — they share the same price signal.
    freq = f.peak_price_frequency or 0.0
    if freq >= _PEAK_PRICE_FREQ_THRESHOLD and s.price_volatility_risk < 65.0:
        return None
    severity = s.price_volatility_risk
    vol = f.price_volatility
    avg = f.avg_price
    if vol is not None and avg is not None and avg > 0:
        cv = vol / avg
        detail = (
            f"Price coefficient of variation is {cv:.0%} "
            f"(std ${vol:.4f}/kWh on a mean of ${avg:.4f}/kWh) — "
            f"window selection carries meaningful pricing uncertainty."
        )
    else:
        detail = (
            f"Price volatility risk score is {s.price_volatility_risk:.0f}/100, "
            f"indicating the price signal is not stable across the forecast window."
        )
    return ("high_price_volatility", "High price volatility", severity, "risk", detail)


def _eval_unstable_carbon(f: FeasibilityFeatures, s: FeasibilityScores) -> _CandidateTuple:
    if f.avg_carbon is None or f.carbon_volatility is None:
        return None
    if s.carbon_instability_risk < _CARBON_INSTABILITY_THRESHOLD:
        return None
    severity = s.carbon_instability_risk
    vol = f.carbon_volatility
    avg = f.avg_carbon
    if vol is not None and avg is not None and avg > 0:
        cv = vol / avg
        detail = (
            f"Carbon coefficient of variation is {cv:.0%} "
            f"(std {vol:.1f} g CO₂/kWh on a mean of {avg:.1f} g CO₂/kWh) — "
            f"clean windows may be short-lived or unpredictable."
        )
    else:
        detail = (
            f"Carbon instability risk score is {s.carbon_instability_risk:.0f}/100, "
            f"indicating the carbon signal varies materially across the forecast window."
        )
    return ("unstable_carbon_profile", "Unstable carbon profile", severity, "risk", detail)


def _eval_limited_favorable_windows(f: FeasibilityFeatures, s: FeasibilityScores) -> _CandidateTuple:
    coverage = f.favorable_window_coverage
    avail = f.available_favorable_window_hours
    required = f.compute_hours_required

    # Trigger when coverage is available and low, OR when avail==0.
    if coverage is None:
        if avail == 0:
            severity = 90.0
            detail = (
                f"No intervals within the deadline window are simultaneously cheap "
                f"and clean, given the {required}h compute requirement."
            )
            return ("limited_favorable_windows", "Limited favorable windows", severity, "risk", detail)
        return None

    if coverage >= _LIMITED_COVERAGE_THRESHOLD:
        return None

    severity = min((1.0 - coverage) * 100.0, 100.0)
    avail_str = f"{avail}" if avail is not None else "few"
    detail = (
        f"Only {avail_str} favorable intervals (cheap + clean) are available before the deadline, "
        f"covering {coverage:.0%} of the {required}h compute requirement."
    )
    return ("limited_favorable_windows", "Limited favorable windows", severity, "risk", detail)


def _eval_large_load(f: FeasibilityFeatures, s: FeasibilityScores) -> _CandidateTuple:
    if f.relative_load_bucket != "heavy":
        return None
    severity = 70.0
    kwh = f.load_energy_required_kwh
    kw = f.machine_kw
    detail = (
        f"The workload requires {kwh:.1f} kWh total ({kw:.2f} kW × {f.compute_hours_required}h), "
        f"placing it in the heavy load tier — fewer windows can absorb the full run."
    )
    return ("large_load_requirement", "Large load requirement", severity, "risk", detail)


def _eval_grid_stress(f: FeasibilityFeatures, s: FeasibilityScores) -> _CandidateTuple:
    if s.grid_stress_score < _GRID_STRESS_THRESHOLD:
        return None
    severity = s.grid_stress_score
    proxy = f.grid_stress_proxy
    if proxy is not None:
        hcs = f.high_carbon_share
        pss = f.price_spike_share
        if hcs is not None and pss is not None:
            detail = (
                f"Grid stress is elevated: {hcs:.0%} of intervals are high-carbon "
                f"and {pss:.0%} are high-price simultaneously."
            )
        else:
            detail = f"Grid stress proxy is {proxy:.2f} — the grid shows combined price and carbon pressure."
    else:
        detail = f"Grid stress score is {s.grid_stress_score:.0f}/100, indicating elevated combined grid pressure."
    return ("grid_stress", "Grid congestion", severity, "risk", detail)


# Opportunity drivers (direction="opportunity")

def _eval_ample_window(f: FeasibilityFeatures, s: FeasibilityScores) -> _CandidateTuple:
    tightness = f.deadline_tightness
    if tightness is None or tightness >= 0.25:
        return None
    severity = (1.0 - tightness) * 100.0
    hours = f.hours_until_deadline
    required = f.compute_hours_required
    if hours is not None:
        detail = (
            f"The {required}h workload has {hours:.1f}h of scheduling window available "
            f"({tightness:.0%} density) — the optimizer has substantial flexibility."
        )
    else:
        detail = (
            f"Deadline tightness is {tightness:.2f} — the workload occupies a small share "
            f"of the available window, giving the optimizer strong scheduling latitude."
        )
    return ("ample_scheduling_window", "Ample scheduling window", severity, "opportunity", detail)


def _eval_abundant_favorable(f: FeasibilityFeatures, s: FeasibilityScores) -> _CandidateTuple:
    coverage = f.favorable_window_coverage
    if coverage is None or coverage < 1.5:
        return None
    severity = min(coverage / 2.0 * 100.0, 100.0)
    avail = f.available_favorable_window_hours
    required = f.compute_hours_required
    avail_str = f"{avail}" if avail is not None else "multiple"
    detail = (
        f"{avail_str} favorable intervals (cheap + clean) are available before the deadline — "
        f"{coverage:.1f}× the {required}h compute requirement."
    )
    return ("abundant_favorable_windows", "Abundant favorable windows", severity, "opportunity", detail)


# Master evaluator list — order is irrelevant; ranking is by severity.
_EVALUATORS = [
    _eval_tight_deadline,
    _eval_high_runtime_density,
    _eval_timing_mismatch,
    _eval_frequent_peak_pricing,
    _eval_high_price_volatility,
    _eval_unstable_carbon,
    _eval_limited_favorable_windows,
    _eval_large_load,
    _eval_grid_stress,
    _eval_ample_window,
    _eval_abundant_favorable,
]


# ---------------------------------------------------------------------------
# Ranking and selection
# ---------------------------------------------------------------------------

def _evaluate_all_drivers(
    features: FeasibilityFeatures,
    scores: FeasibilityScores,
) -> list[tuple[str, str, float, DriverDirection, str]]:
    """Run all evaluators and collect triggered candidates."""
    results = []
    for fn in _EVALUATORS:
        candidate = fn(features, scores)
        if candidate is not None:
            results.append(candidate)
    return results


def _rank_and_select(
    candidates: list[tuple[str, str, float, DriverDirection, str]],
) -> list[FeasibilityDriver]:
    """
    Sort candidates by severity (desc), apply MIN/MAX bounds, assign ranks.

    Risk drivers always appear before opportunity drivers at equal severity,
    because the screening context is primarily about friction, not upsides.
    Opportunity drivers appear at the end of the list regardless of severity.
    """
    risks = [(k, l, sv, d, det) for k, l, sv, d, det in candidates if d == "risk"]
    opps  = [(k, l, sv, d, det) for k, l, sv, d, det in candidates if d == "opportunity"]

    risks.sort(key=lambda x: x[2], reverse=True)
    opps.sort(key=lambda x: x[2], reverse=True)

    # Take up to MAX_DRIVERS total; always include at least one opportunity if present.
    risk_slots = _MAX_DRIVERS - (1 if opps else 0)
    selected_risks = risks[:risk_slots]
    selected_opps = opps[:1]  # at most one opportunity driver per run

    combined = selected_risks + selected_opps

    # Pad to MIN_DRIVERS with whichever pool has entries, to satisfy the floor.
    if len(combined) < _MIN_DRIVERS:
        remaining_risks = risks[risk_slots:]
        remaining_opps = opps[1:]
        for item in (remaining_risks + remaining_opps)[:_MIN_DRIVERS - len(combined)]:
            combined.append(item)

    return [
        FeasibilityDriver(
            key=key,
            label=label,
            rank=i + 1,
            severity=round(severity, 1),
            direction=direction,
            detail=detail,
        )
        for i, (key, label, severity, direction, detail) in enumerate(combined)
    ]


# ---------------------------------------------------------------------------
# Interpretation text
# ---------------------------------------------------------------------------

def _build_interpretation(
    label: OverallLabel,
    drivers: list[FeasibilityDriver],
    features: FeasibilityFeatures,
    scores: FeasibilityScores,
) -> str:
    """
    Build a 1–2 sentence interpretation grounded in the top drivers.
    Varies by overall_label and the identity of the top 1–2 risk drivers.
    """
    if not drivers:
        return "Insufficient data to interpret the scheduling environment for this workload."

    top_risk_keys = [d.key for d in drivers if d.direction == "risk"]
    primary_key = top_risk_keys[0] if top_risk_keys else drivers[0].key

    if label == "Infeasible":
        tightness = features.deadline_tightness
        if tightness is not None and tightness >= 1.0:
            return (
                f"The workload cannot be scheduled before the deadline: "
                f"it requires {features.compute_hours_required}h but deadline_tightness is "
                f"{tightness:.2f} (available intervals do not cover the compute requirement). "
                f"Extend the deadline or reduce compute hours to proceed."
            )
        return (
            "The deadline window cannot be resolved for this workload. "
            "Verify the deadline format and that the forecast covers the required horizon."
        )

    if label == "Strong":
        opp_keys = [d.key for d in drivers if d.direction == "opportunity"]
        if "ample_scheduling_window" in opp_keys:
            return (
                f"This workload has strong scheduling conditions: feasibility score is "
                f"{scores.feasibility_score:.0f}/100 with {features.hours_until_deadline:.1f}h "
                f"of window against a {features.compute_hours_required}h compute requirement. "
                f"The optimizer has wide latitude to find low-cost, low-carbon windows."
                if features.hours_until_deadline is not None else
                f"This workload has strong scheduling conditions: feasibility score is "
                f"{scores.feasibility_score:.0f}/100 with low deadline pressure and "
                f"favorable grid signal coverage."
            )
        return (
            f"Scheduling conditions are favorable: feasibility score is "
            f"{scores.feasibility_score:.0f}/100 and no significant risk factors were detected. "
            f"The optimizer is well-positioned to find an efficient run window."
        )

    # Moderate and Marginal — reference the primary driver directly.
    primary_detail = next((d.detail for d in drivers if d.key == primary_key), "")
    second_key = top_risk_keys[1] if len(top_risk_keys) > 1 else None

    _INTERPRETATION_LEAD = {
        "tight_deadline": "Deadline pressure is the primary constraint.",
        "high_runtime_density": "Runtime density is the primary friction source.",
        "timing_constraint_mismatch": "Favorable windows are misaligned with the deadline.",
        "frequent_peak_pricing": "Peak-price concentration is the primary risk.",
        "high_price_volatility": "Price signal instability is the primary concern.",
        "unstable_carbon_profile": "Carbon signal instability is the primary concern.",
        "limited_favorable_windows": "Limited favorable scheduling windows are the primary constraint.",
        "large_load_requirement": "Load size is constraining window selection.",
        "grid_stress": "Combined grid stress is elevated across the forecast window.",
    }
    lead = _INTERPRETATION_LEAD.get(primary_key, "Scheduling friction is elevated.")

    if second_key:
        _SECOND_BRIDGE = {
            "high_price_volatility": f"Price volatility (risk score {scores.price_volatility_risk:.0f}/100) adds further uncertainty.",
            "unstable_carbon_profile": f"Carbon instability (risk score {scores.carbon_instability_risk:.0f}/100) compounds the constraint.",
            "limited_favorable_windows": f"Favorable window coverage of {features.favorable_window_coverage:.0%} leaves limited fallback options." if features.favorable_window_coverage is not None else "Few favorable windows are available as backup.",
            "timing_constraint_mismatch": "The best available windows are concentrated late in the scheduling horizon.",
            "frequent_peak_pricing": f"Peak-price frequency of {features.peak_price_frequency:.0%} further reduces low-cost options." if features.peak_price_frequency is not None else "Frequent peak-price intervals reduce flexibility.",
            "tight_deadline": f"Deadline tightness of {features.deadline_tightness:.0%} leaves little room to wait." if features.deadline_tightness is not None else "The deadline leaves little room to avoid suboptimal windows.",
            "large_load_requirement": f"The {features.load_energy_required_kwh:.1f} kWh requirement narrows usable windows.",
            "grid_stress": f"Elevated grid stress (score {scores.grid_stress_score:.0f}/100) limits clean scheduling options.",
        }
        bridge = _SECOND_BRIDGE.get(second_key, f"A secondary risk factor ({second_key.replace('_', ' ')}) also applies.")
        return f"{lead} {bridge}"

    return f"{lead} {primary_detail}"


# ---------------------------------------------------------------------------
# Recommendation context
# ---------------------------------------------------------------------------

def _build_recommendation_context(
    label: OverallLabel,
    drivers: list[FeasibilityDriver],
    features: FeasibilityFeatures,
    scores: FeasibilityScores,
) -> str:
    """
    One sentence that frames what the user should do or expect.
    Varies by overall_label; references concrete numbers where available.
    """
    top_risk_keys = [d.key for d in drivers if d.direction == "risk"]
    primary_key = top_risk_keys[0] if top_risk_keys else None

    if label == "Infeasible":
        return (
            f"Reduce the compute requirement below {features.available_favorable_window_hours or features.compute_hours_required}h "
            f"or extend the deadline to make this workload schedulable."
            if features.available_favorable_window_hours is not None
            else "Extend the deadline or reduce compute hours before running the optimizer."
        )

    if label == "Strong":
        feasibility = scores.feasibility_score
        return (
            f"Run the optimizer now — feasibility score of {feasibility:.0f}/100 indicates "
            f"conditions are well-suited for an efficient scheduled run."
        )

    if label == "Moderate":
        if primary_key in ("tight_deadline", "high_runtime_density"):
            tightness = features.deadline_tightness
            return (
                f"The optimizer can still find improved windows, but deadline tightness of "
                f"{tightness:.0%} limits how much it can shift the schedule."
                if tightness is not None
                else "The optimizer can find improved windows, though scheduling flexibility is reduced."
            )
        if primary_key in ("limited_favorable_windows", "timing_constraint_mismatch"):
            coverage = features.favorable_window_coverage
            return (
                f"Favorable window coverage of {coverage:.0%} means the optimizer may need to "
                f"accept some suboptimal intervals to complete the full {features.compute_hours_required}h run."
                if coverage is not None
                else f"Limited favorable windows mean the optimizer may need to accept some suboptimal intervals."
            )
        if primary_key in ("high_price_volatility", "frequent_peak_pricing"):
            return (
                f"Price signal instability (risk score {scores.price_volatility_risk:.0f}/100) "
                f"means scheduled savings may vary — run the optimizer to find the current best window."
            )
        return (
            f"Friction score is {scores.friction_score:.0f}/100 — the optimizer can find "
            f"improvement, though conditions are not ideal."
        )

    # Marginal
    if primary_key in ("tight_deadline", "high_runtime_density", "limited_favorable_windows"):
        tightness = features.deadline_tightness
        return (
            f"Scheduling margin is narrow (deadline tightness {tightness:.0%}); "
            f"the optimizer will run but savings potential is constrained."
            if tightness is not None
            else "Scheduling margin is narrow; the optimizer will run but savings potential is limited."
        )
    if primary_key in ("frequent_peak_pricing", "high_price_volatility", "grid_stress"):
        return (
            f"Grid conditions are challenging (friction score {scores.friction_score:.0f}/100); "
            f"running the optimizer is still recommended to find the least-cost available window."
        )
    return (
        f"Friction score is {scores.friction_score:.0f}/100 — proceed with the optimizer "
        f"but expect constrained improvement over a naive run-now schedule."
    )


# ---------------------------------------------------------------------------
# Business recommendation
# ---------------------------------------------------------------------------

# Tunable thresholds for recommendation category assignment.
# These operate on the three combined scores (all 0–100).

_REC_PRIORITIZE_MIN_FEASIBILITY: float = 65.0
_REC_PRIORITIZE_MAX_FRICTION: float = 40.0
_REC_PRIORITIZE_MAX_DELAY_RISK: float = 45.0

_REC_PROMISING_MIN_FEASIBILITY: float = 45.0
_REC_PROMISING_MAX_FRICTION: float = 58.0

_REC_CAUTION_MIN_FEASIBILITY: float = 20.0
# Below _REC_CAUTION_MIN_FEASIBILITY or overall_label == "Infeasible" → Deprioritize

RecommendationCategory = Literal["Prioritize", "Promising but monitor", "Caution", "Deprioritize"]


@dataclass(frozen=True)
class FeasibilityRecommendation:
    """Business-facing recommendation produced from the scored feasibility result."""

    category: RecommendationCategory
    """Top-level decision label.
    ``"Prioritize"`` → run now;
    ``"Promising but monitor"`` → run, but check one thing;
    ``"Caution"`` → proceed with reduced expectations;
    ``"Deprioritize"`` → do not run until conditions change."""

    headline: str
    """One short phrase (≤10 words). Suitable for a card title or badge label."""

    body: str
    """2–3 sentences of business context grounded in actual scores and drivers.
    No engineering jargon; no metric labels like 'friction_score'."""

    action: str
    """One imperative sentence — the specific next step for the user."""


def build_recommendation(
    features: FeasibilityFeatures,
    scores: FeasibilityScores,
    explanation: FeasibilityExplanation,
) -> FeasibilityRecommendation:
    """
    Derive a business-style recommendation from scored feasibility output.

    Evaluation order (first match wins):
      1. Infeasible label                         → Deprioritize
      2. High feasibility + low friction/delay    → Prioritize
      3. Moderate feasibility + moderate friction → Promising but monitor
      4. Low-but-viable feasibility               → Caution
      5. Default (very low feasibility)           → Deprioritize

    Parameters
    ----------
    features:
        Output of ``extract_feasibility_features()``.
    scores:
        Output of ``calculate_feasibility_scores()``.
    explanation:
        Output of ``generate_feasibility_explanation()``.  The ``overall_label``
        and ``drivers`` are used to vary recommendation text.

    Returns
    -------
    FeasibilityRecommendation
        Always complete; never raises.
    """
    category = _assign_category(scores, explanation)
    primary_driver_key = next(
        (d.key for d in explanation.drivers if d.direction == "risk"),
        None,
    )
    return _build_rec(category, primary_driver_key, features, scores, explanation)


# ---------------------------------------------------------------------------
# Category assignment
# ---------------------------------------------------------------------------

def _assign_category(
    scores: FeasibilityScores,
    explanation: FeasibilityExplanation,
) -> RecommendationCategory:
    """
    Map combined scores to a recommendation category.

    Decision rules (evaluated top to bottom; first match wins):

      Deprioritize (hard gate):
        overall_label == "Infeasible"

      Prioritize:
        feasibility_score >= 65  AND
        friction_score    <= 40  AND
        delay_risk_score  <= 45

      Promising but monitor:
        feasibility_score >= 45  AND
        friction_score    <= 58

      Caution:
        feasibility_score >= 20

      Deprioritize (default):
        feasibility_score < 20

    All thresholds are defined as module-level constants prefixed ``_REC_``.
    """
    if explanation.overall_label == "Infeasible":
        return "Deprioritize"

    f  = scores.feasibility_score
    fr = scores.friction_score
    dr = scores.delay_risk_score

    if (
        f  >= _REC_PRIORITIZE_MIN_FEASIBILITY
        and fr <= _REC_PRIORITIZE_MAX_FRICTION
        and dr <= _REC_PRIORITIZE_MAX_DELAY_RISK
    ):
        return "Prioritize"

    if f >= _REC_PROMISING_MIN_FEASIBILITY and fr <= _REC_PROMISING_MAX_FRICTION:
        return "Promising but monitor"

    if f >= _REC_CAUTION_MIN_FEASIBILITY:
        return "Caution"

    return "Deprioritize"


# ---------------------------------------------------------------------------
# Text construction
# ---------------------------------------------------------------------------

def _build_rec(
    category: RecommendationCategory,
    primary_driver_key: str | None,
    features: FeasibilityFeatures,
    scores: FeasibilityScores,
    explanation: FeasibilityExplanation,
) -> FeasibilityRecommendation:
    """Dispatch to the right text builder for each category."""
    if category == "Prioritize":
        return _rec_prioritize(features, scores)
    if category == "Promising but monitor":
        return _rec_promising(primary_driver_key, features, scores)
    if category == "Caution":
        return _rec_caution(primary_driver_key, features, scores)
    return _rec_deprioritize(explanation.overall_label, features, scores)


def _rec_prioritize(
    features: FeasibilityFeatures,
    scores: FeasibilityScores,
) -> FeasibilityRecommendation:
    f  = scores.feasibility_score
    dr = scores.delay_risk_score

    if features.hours_until_deadline is not None:
        body = (
            f"This workload is a strong fit for optimized scheduling. "
            f"There is {features.hours_until_deadline:.1f}h of scheduling window for a "
            f"{features.compute_hours_required}h run, and grid conditions show enough "
            f"variation to make window selection worthwhile. "
            f"Delay risk is low ({dr:.0f}/100), so there is no pressure to commit immediately — "
            f"but conditions are favorable now."
        )
    else:
        body = (
            f"This workload is a strong fit for optimized scheduling. "
            f"Grid conditions show enough variation to make window selection worthwhile, "
            f"and the deadline allows meaningful flexibility. "
            f"Delay risk is low ({dr:.0f}/100) — conditions are favorable now."
        )

    return FeasibilityRecommendation(
        category="Prioritize",
        headline="Strong candidate — run the optimizer now",
        body=body,
        action=(
            "Submit this workload to the optimizer. "
            "Favorable windows are available and no urgent constraints require immediate action."
        ),
    )


def _rec_promising(
    primary_driver_key: str | None,
    features: FeasibilityFeatures,
    scores: FeasibilityScores,
) -> FeasibilityRecommendation:
    f  = scores.feasibility_score
    dr = scores.delay_risk_score

    # Select a context-specific body variant based on what's actually constraining the run.
    _timing_keys = {"tight_deadline", "high_runtime_density", "timing_constraint_mismatch"}
    _market_keys = {"high_price_volatility", "frequent_peak_pricing", "unstable_carbon_profile"}
    _window_keys = {"limited_favorable_windows", "large_load_requirement"}

    if primary_driver_key in _timing_keys:
        tightness = features.deadline_tightness
        constraint_phrase = (
            f"deadline pressure ({tightness:.0%} scheduling density)"
            if tightness is not None
            else "deadline pressure"
        )
        body = (
            f"This workload has solid optimization potential, but {constraint_phrase} means "
            f"the available scheduling window will narrow over time. "
            f"Delay risk is {dr:.0f}/100 — waiting for better conditions is likely to reduce "
            f"options rather than improve them."
        )
        action = (
            "Run the optimizer soon. "
            "Deadline pressure will reduce scheduling flexibility the longer you wait."
        )
    elif primary_driver_key in _market_keys:
        body = (
            f"This workload has meaningful optimization potential (feasibility {f:.0f}/100), "
            f"though market signal variability adds some uncertainty to the expected savings. "
            f"The optimizer can still find a better window than running immediately."
        )
        action = (
            "Run the optimizer now. "
            "Validate the recommended window against current conditions before committing the run."
        )
    elif primary_driver_key in _window_keys:
        coverage = features.favorable_window_coverage
        coverage_phrase = (
            f"favorable window coverage is {coverage:.0%} of the compute requirement"
            if coverage is not None
            else "favorable window availability is limited relative to the compute requirement"
        )
        body = (
            f"Optimization is viable, but {coverage_phrase}. "
            f"The optimizer will identify the best available window, though not all intervals "
            f"will meet ideal cost and carbon thresholds."
        )
        action = (
            "Proceed with the optimizer. "
            "Review the recommended schedule to confirm it meets your cost and carbon targets."
        )
    else:
        body = (
            f"This workload has good optimization potential (feasibility {f:.0f}/100) "
            f"with manageable constraints. "
            f"The optimizer is well-positioned to find improvement over an immediate start."
        )
        action = "Run the optimizer. Review the recommended schedule before committing."

    return FeasibilityRecommendation(
        category="Promising but monitor",
        headline="Good opportunity — proceed with awareness",
        body=body,
        action=action,
    )


def _rec_caution(
    primary_driver_key: str | None,
    features: FeasibilityFeatures,
    scores: FeasibilityScores,
) -> FeasibilityRecommendation:
    f  = scores.feasibility_score
    fr = scores.friction_score
    dr = scores.delay_risk_score

    _timing_keys = {"tight_deadline", "high_runtime_density"}
    _market_keys = {"high_price_volatility", "frequent_peak_pricing", "grid_stress"}
    _window_keys = {"limited_favorable_windows", "timing_constraint_mismatch"}

    if primary_driver_key in _timing_keys:
        tightness = features.deadline_tightness
        body = (
            f"Optimization potential is limited by scheduling pressure. "
            f"The deadline allows little room to avoid suboptimal windows"
            + (f" (scheduling density is {tightness:.0%})" if tightness is not None else "")
            + f", and delay risk is elevated ({dr:.0f}/100). "
            f"The optimizer will still outperform a naive immediate start, "
            f"but do not expect the same savings as a less time-constrained workload."
        )
        action = (
            "Run the optimizer now rather than waiting — "
            "delay will further reduce scheduling options without improving conditions."
        )
    elif primary_driver_key in _market_keys:
        body = (
            f"Grid conditions are challenging across the forecast window. "
            f"High friction ({fr:.0f}/100) means most intervals carry elevated cost or carbon, "
            f"limiting the optimizer's ability to find materially cleaner or cheaper windows. "
            f"Running the optimizer is still preferable to skipping it entirely."
        )
        action = (
            "Proceed with the optimizer to identify the least-bad available window. "
            "Consider whether deferring the workload to a different forecast period is practical."
        )
    elif primary_driver_key in _window_keys:
        coverage = features.favorable_window_coverage
        body = (
            f"Few intervals meet both cost and carbon thresholds for this workload"
            + (
                f" (favorable coverage is {coverage:.0%} of the compute requirement)"
                if coverage is not None
                else ""
            )
            + f". "
            f"The optimizer can still improve over a random start, "
            f"but savings will be constrained by the limited selection of favorable windows."
        )
        action = (
            "Run the optimizer to capture whatever favorable windows exist. "
            "Manage expectations on savings magnitude given the current window availability."
        )
    else:
        body = (
            f"Scheduling friction is elevated (feasibility {f:.0f}/100), "
            f"which limits the optimizer's expected improvement. "
            f"The workload can be scheduled, but conditions are not well-suited for "
            f"significant cost or carbon savings relative to an immediate start."
        )
        action = (
            "Proceed with the optimizer if schedule flexibility allows. "
            "Revisit workload parameters if targets require more meaningful savings."
        )

    return FeasibilityRecommendation(
        category="Caution",
        headline="Limited opportunity — proceed with realistic expectations",
        body=body,
        action=action,
    )


def _rec_deprioritize(
    overall_label: str,
    features: FeasibilityFeatures,
    scores: FeasibilityScores,
) -> FeasibilityRecommendation:
    if overall_label == "Infeasible":
        tightness = features.deadline_tightness
        if tightness is not None and tightness >= 1.0:
            required = features.compute_hours_required
            avail = features.available_favorable_window_hours
            body = (
                f"This workload cannot be scheduled before the deadline as configured. "
                f"The {required}h compute requirement exceeds the available scheduling window"
                + (
                    f" — only {avail}h of favorable intervals exist before the cutoff"
                    if avail is not None
                    else ""
                )
                + f". "
                f"The optimizer has no viable solution space to work within."
            )
            action = (
                "Extend the deadline or reduce compute hours, then rerun screening. "
                "Do not submit this workload to the optimizer in its current form."
            )
        else:
            body = (
                "The deadline window for this workload could not be resolved against the "
                "available forecast data. Optimization cannot be attempted without a valid "
                "scheduling horizon."
            )
            action = (
                "Verify the deadline format and that the forecast covers the required time range, "
                "then rerun screening."
            )
        return FeasibilityRecommendation(
            category="Deprioritize",
            headline="Not schedulable as configured",
            body=body,
            action=action,
        )

    # Marginal but not technically infeasible — very low feasibility score.
    f = scores.feasibility_score
    body = (
        f"Current grid and scheduling conditions make meaningful optimization unlikely. "
        f"With a feasibility score of {f:.0f}/100, the optimizer is unlikely to find a "
        f"window that improves materially over an immediate start. "
        f"Running now would produce a result, but the scheduling signal is too weak to justify "
        f"delaying the workload in search of a better window."
    )
    action = (
        "Defer this workload or run it immediately without optimization. "
        "Rerun screening when grid conditions improve or deadline pressure is reduced."
    )
    return FeasibilityRecommendation(
        category="Deprioritize",
        headline="Optimization not recommended under current conditions",
        body=body,
        action=action,
    )
