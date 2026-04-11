from __future__ import annotations

import json
from typing import Any

from src.services.ai.schemas import AiInterpretRequest, InterpretRequest


SYSTEM_PROMPT = """\
You are an infrastructure operations analyst writing a decision memo for Util, a compute scheduling optimizer.

Your job: interpret the optimizer output and produce structured, judgment-backed analysis.
You will populate four judgment fields first, then synthesize them into a single memo paragraph.
Populate the structured fields in order — your summary must be informed by those judgments, not written independently.

━━━ JUDGMENT CRITERIA ━━━

tradeoff_strength  — Magnitude of the improvement the optimizer achieved.
  Compute the % difference between selected_result and the baseline alternative on the
  selected_objective's primary metric (cost → projected_cost, carbon → projected_emissions,
  balanced → both). Use: abs(selected - baseline) / baseline * 100.
  "clear"    : >25% improvement
  "moderate" : 10–25% improvement
  "marginal" : <10% improvement
  "none"     : no alternative provided

decision_confidence  — How strongly the data supports choosing this result.
  "high"   : tradeoff is clear or moderate AND the schedule window fits before the deadline
  "low"    : tradeoff is marginal, OR the alternative is competitive on the non-selected objective
  "medium" : all other cases

objective_driver  — Whether the result reflects real optimizer freedom or deadline/constraint pressure.
  Base this on the % improvement on the selected objective.
  "objective"  : improvement >15% — the optimizer found a materially better window
  "constraint" : improvement <5%  — limited options suggest deadline or interval count was binding
  "mixed"      : improvement 5–15%

alternative_attractiveness  — How appealing the baseline alternative is to a user with different priorities.
  For cost-optimized runs: compare projected_emissions between selected and baseline.
  For carbon-optimized runs: compare projected_cost between selected and baseline.
  For balanced runs: assess whether either dimension is notably better in the alternative.
  "meaningful" : the alternative is >15% better on the non-selected dimension
  "marginal"   : the alternative is 1–15% better on the non-selected dimension
  "none"       : no alternative, OR the alternative is worse on all dimensions

━━━ MEMO PARAGRAPH RULES ━━━

Write the summary as 3–5 sentences in the style of a short ops memo:
  S1: State the objective, the numeric outcome, and your tradeoff_strength verdict.
  S2: Compare selected vs baseline numerically. If tradeoff is marginal, name that explicitly.
  S3: State your objective_driver verdict and what it implies — did the optimizer have real leverage, or was it constrained?
  S4: State alternative_attractiveness. Name specifically what type of user would prefer the alternative, based only on the data provided.
  S5 (optional): State decision_confidence and the single clearest actionable takeaway.

━━━ HARD RULES ━━━

- Every claim must be directly traceable to a field in the input JSON.
- Do not reference market conditions, grid behavior, time-of-day patterns, or pricing conventions
  unless those exact labels appear in the input.
- Do not use hedging language: "likely", "probably", "typically", "generally", "usually",
  "often", "suggests", "indicates", "may", "might", "could", or any equivalent.
- Do not infer causes for numeric values — state them as given facts.
- If a field is absent from the input, omit any claim that depends on it.
- Return ONLY valid JSON. No prose, markdown, or code fences outside the JSON.

━━━ OUTPUT SCHEMA ━━━

{
  "tradeoff_strength": "clear | moderate | marginal | none",
  "decision_confidence": "high | medium | low",
  "objective_driver": "objective | constraint | mixed",
  "alternative_attractiveness": "meaningful | marginal | none",
  "summary": "The memo paragraph synthesizing all four judgments above."
}
"""


def build_interpret_prompt(request: AiInterpretRequest) -> str:
    """
    Build a compact, grounded prompt from an AiInterpretRequest.

    Only includes facts drawn from the provided data.
    All values pass through json.dumps to prevent prompt injection.
    """
    facts: dict[str, Any] = {
        "selected_objective": request.selected_objective,
        "region": request.region,
        "deadline": request.deadline,
        "selected_result": request.selected_result.model_dump(exclude_none=True),
    }

    if request.alternatives:
        facts["alternatives"] = [
            alt.model_dump(exclude_none=True) for alt in request.alternatives
        ]

    return (
        "Interpret the following optimizer result. "
        "Populate tradeoff_strength, decision_confidence, objective_driver, and "
        "alternative_attractiveness first, then write summary. "
        "Return JSON only:\n\n"
        + json.dumps(facts, indent=2, default=str)
    )


def _extract_scenario_facts(scenario: InterpretRequest) -> dict[str, Any]:
    """Compact fact dict for a comparison scenario."""
    m = scenario.metrics
    s = scenario.schedule
    result: dict[str, Any] = {
        "objective": scenario.objective,
        "schedule_mode": scenario.schedule_mode,
        "region": scenario.location.resolved_region,
        "window_start": s.recommended_window_start,
        "window_end": s.recommended_window_end,
        "selected_intervals": s.selected_interval_count,
    }
    if m.cost_savings is not None:
        result["cost_savings_usd"] = round(m.cost_savings, 4)
    if m.carbon_savings_kg is not None:
        result["carbon_savings_kg"] = round(m.carbon_savings_kg, 4)
    if m.cost_reduction_pct is not None:
        result["cost_reduction_pct"] = round(m.cost_reduction_pct, 2)
    if m.carbon_reduction_pct is not None:
        result["carbon_reduction_pct"] = round(m.carbon_reduction_pct, 2)
    return result
