from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.analysis.feasibility_explanations import (
    FeasibilityExplanation,
    FeasibilityRecommendation,
    build_recommendation,
    generate_feasibility_explanation,
)
from src.analysis.feasibility_features import FeasibilityFeatures, extract_feasibility_features
from src.analysis.feasibility_scoring import FeasibilityScores, calculate_feasibility_scores


@dataclass(frozen=True)
class FeasibilityResult:
    """Complete opportunity screening output for one optimizer run."""

    zip_code: str
    region: str | None
    compute_hours_required: int
    deadline: str
    machine_watts: int
    features: FeasibilityFeatures
    scores: FeasibilityScores
    explanation: FeasibilityExplanation
    recommendation: FeasibilityRecommendation
    forecast_df: pd.DataFrame


def run_feasibility_from_pipeline_result(
    pipeline_result: dict[str, Any],
    deadline_str: str,
) -> FeasibilityResult:
    """Build opportunity screening from an existing pipeline result."""
    workload = pipeline_result["workload_input"]
    forecast_df: pd.DataFrame = pipeline_result["forecast"]

    features = extract_feasibility_features(
        forecast_df=forecast_df,
        compute_hours_required=workload.compute_hours_required,
        deadline=deadline_str,
        machine_watts=workload.machine_watts,
    )
    scores = calculate_feasibility_scores(features)
    explanation = generate_feasibility_explanation(features, scores)
    recommendation = build_recommendation(features, scores, explanation)

    return FeasibilityResult(
        zip_code=workload.zip_code,
        region=pipeline_result.get("region"),
        compute_hours_required=workload.compute_hours_required,
        deadline=deadline_str,
        machine_watts=workload.machine_watts,
        features=features,
        scores=scores,
        explanation=explanation,
        recommendation=recommendation,
        forecast_df=forecast_df,
    )


def serialize_feasibility_result(result: FeasibilityResult) -> dict[str, Any]:
    """Return the V1 API payload for Opportunity Screening."""
    scores = result.scores
    explanation = result.explanation
    recommendation = result.recommendation

    return {
        "recommendation": {
            "category": recommendation.category,
            "headline": recommendation.headline,
            "body": recommendation.body,
            "action": recommendation.action,
        },
        "summary": {
            "overall_label": explanation.overall_label,
            "feasibility_score": _round_or_none(scores.feasibility_score),
            "friction_score": _round_or_none(scores.friction_score),
            "delay_risk_score": _round_or_none(scores.delay_risk_score),
            "feasibility_bucket": scores.feasibility_bucket,
            "friction_bucket": scores.friction_bucket,
            "delay_risk_bucket": scores.delay_risk_bucket,
        },
        "component_scores": {
            "grid_stress_score": _round_or_none(scores.grid_stress_score),
            "price_volatility_risk": _round_or_none(scores.price_volatility_risk),
            "carbon_instability_risk": _round_or_none(scores.carbon_instability_risk),
            "timing_risk": _round_or_none(scores.timing_risk),
            "load_pressure_score": _round_or_none(scores.load_pressure_score),
        },
        "drivers": [
            {
                "key": driver.key,
                "label": driver.label,
                "rank": driver.rank,
                "severity": driver.severity,
                "direction": driver.direction,
                "detail": driver.detail,
            }
            for driver in explanation.drivers
        ],
        "interpretation": explanation.interpretation,
    }


def _round_or_none(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None else None
