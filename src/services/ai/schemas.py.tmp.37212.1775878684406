from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# AI-specific request / response schemas
# These are intentionally decoupled from the optimizer input schema.
# POST /api/v1/ai/interpret uses ONLY these models.
# ---------------------------------------------------------------------------

class ScenarioResult(BaseModel):
    """A single optimizer result passed to the AI layer for interpretation."""
    objective: str
    projected_cost: float | None = None
    projected_emissions: float | None = None
    schedule_summary: str | None = None
    # Accept any extra fields the frontend may include without failing validation.
    model_config = {"extra": "allow"}


class AiInterpretRequest(BaseModel):
    """
    Lightweight AI interpretation request.
    Contains ONLY the fields needed to explain and compare optimizer outputs.
    Does NOT require any optimizer input fields (objective, schedule_mode, etc.).
    """
    selected_objective: str
    deadline: str | None = None
    region: str | None = None
    selected_result: ScenarioResult
    alternatives: list[ScenarioResult] = Field(default_factory=list)


class AiInterpretResponse(BaseModel):
    status: Literal["ok", "unavailable", "error"]

    # Primary display field — single polished memo paragraph.
    summary: str | None = None

    # Structured judgment fields (str rather than Literal so unexpected model
    # output never causes a validation crash).
    # Expected values documented below:
    #   tradeoff_strength       : "clear" | "moderate" | "marginal" | "none"
    #   decision_confidence     : "high"  | "medium"   | "low"
    #   objective_driver        : "objective" | "constraint" | "mixed"
    #   alternative_attractiveness : "meaningful" | "marginal" | "none"
    tradeoff_strength: str | None = None
    decision_confidence: str | None = None
    objective_driver: str | None = None
    alternative_attractiveness: str | None = None

    # Legacy sectioned fields kept for backward compatibility.
    why_this_schedule: str | None = None
    tradeoff_summary: str | None = None
    scenario_comparison: str | None = None
    recommendation_memo: str | None = None

    message: str | None = None


# ---------------------------------------------------------------------------
# Legacy internal context models
# Used by comparison_service.py for deeper pipeline-level diffs.
# NOT used by the /api/v1/ai/interpret route.
# ---------------------------------------------------------------------------


class LocationContext(BaseModel):
    resolved_region: str
    location_lookup_status: str


class PricingContext(BaseModel):
    pricing_status_label: str
    pricing_source: str
    pricing_market: str
    pricing_market_label: str


class ForecastContext(BaseModel):
    row_count: int
    window_start: str | None = None
    window_end: str | None = None
    carbon_signal_mix: list[str]


class MetricsContext(BaseModel):
    """
    Key optimizer metrics extracted from the pipeline output.
    All fields are optional to handle partial or demo-mode runs gracefully.
    """
    cost_savings: float | None = None
    cost_reduction_pct: float | None = None
    carbon_savings_kg: float | None = None
    carbon_reduction_pct: float | None = None
    baseline_cost: float | None = None
    optimized_cost: float | None = None
    baseline_carbon_kg: float | None = None
    optimized_carbon_kg: float | None = None


class ScheduleContext(BaseModel):
    recommended_window_start: str | None = None
    recommended_window_end: str | None = None
    selected_interval_count: int
    status: str
    explanation: str


class InterpretRequest(BaseModel):
    objective: Literal["carbon", "cost", "balanced"]
    schedule_mode: Literal["flexible", "block"]
    carbon_estimation_mode: str
    compute_hours_required: int
    machine_watts: int
    location: LocationContext
    pricing: PricingContext
    forecast: ForecastContext
    metrics: MetricsContext
    schedule: ScheduleContext
    scenario_b: Optional[InterpretRequest] = Field(
        default=None,
        description="Optional second scenario for side-by-side comparison.",
    )


# Required for self-referential model in Pydantic v2.
InterpretRequest.model_rebuild()


class InterpretResponse(BaseModel):
    status: Literal["ok", "unavailable", "error"]
    why_this_schedule: str | None = None
    tradeoff_summary: str | None = None
    scenario_comparison: str | None = None
    recommendation_memo: str | None = None
    message: str | None = None
