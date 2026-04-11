from __future__ import annotations

from typing import Any

from src.services.ai.schemas import InterpretRequest


def build_scenario_diff(
    scenario_a: InterpretRequest,
    scenario_b: InterpretRequest,
) -> dict[str, Any]:
    """
    Produce a structured diff of two already-computed optimizer results.

    Operates purely on the provided InterpretRequest payloads — does not
    re-run the optimizer or duplicate any optimizer logic. Intended to
    enrich the AI prompt for scenario_comparison, or to be returned
    directly to the client as a structured diff.
    """
    a = scenario_a.metrics
    b = scenario_b.metrics

    def _diff(val_a: float | None, val_b: float | None) -> float | None:
        if val_a is not None and val_b is not None:
            return round(val_b - val_a, 4)
        return None

    def _pct_diff(val_a: float | None, val_b: float | None) -> float | None:
        if val_a is not None and val_b is not None and val_a != 0:
            return round(((val_b - val_a) / abs(val_a)) * 100, 2)
        return None

    return {
        "scenario_a": {
            "objective": scenario_a.objective,
            "schedule_mode": scenario_a.schedule_mode,
            "region": scenario_a.location.resolved_region,
            "window_start": scenario_a.schedule.recommended_window_start,
            "window_end": scenario_a.schedule.recommended_window_end,
            "cost_savings_usd": a.cost_savings,
            "carbon_savings_kg": a.carbon_savings_kg,
            "cost_reduction_pct": a.cost_reduction_pct,
            "carbon_reduction_pct": a.carbon_reduction_pct,
        },
        "scenario_b": {
            "objective": scenario_b.objective,
            "schedule_mode": scenario_b.schedule_mode,
            "region": scenario_b.location.resolved_region,
            "window_start": scenario_b.schedule.recommended_window_start,
            "window_end": scenario_b.schedule.recommended_window_end,
            "cost_savings_usd": b.cost_savings,
            "carbon_savings_kg": b.carbon_savings_kg,
            "cost_reduction_pct": b.cost_reduction_pct,
            "carbon_reduction_pct": b.carbon_reduction_pct,
        },
        "delta": {
            "cost_savings_diff": _diff(a.cost_savings, b.cost_savings),
            "carbon_savings_diff": _diff(a.carbon_savings_kg, b.carbon_savings_kg),
            "cost_reduction_pct_diff": _pct_diff(
                a.cost_reduction_pct, b.cost_reduction_pct
            ),
            "carbon_reduction_pct_diff": _pct_diff(
                a.carbon_reduction_pct, b.carbon_reduction_pct
            ),
        },
    }
