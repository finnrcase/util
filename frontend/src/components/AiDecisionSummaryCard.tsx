import { useEffect, useRef } from "react";
import { useMutation } from "@tanstack/react-query";
import { SectionCard } from "./SectionCard";
import { interpretOptimization } from "../lib/api";
import type { AiInterpretRequest, AiInterpretResponse, AiScenarioResult, OptimizeResponse } from "../types/api";

// ---------------------------------------------------------------------------
// Payload builder
// ---------------------------------------------------------------------------

function numericOrNull(value: number | string | null | undefined): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  return null;
}

function fmtWindow(start: string | null | undefined, end: string | null | undefined): string | null {
  if (!start || !end) return null;
  const opts: Intl.DateTimeFormatOptions = { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" };
  return `${new Intl.DateTimeFormat("en-US", opts).format(new Date(start))} to ${new Intl.DateTimeFormat("en-US", opts).format(new Date(end))}`;
}

function buildInterpretPayload(run: OptimizeResponse): AiInterpretRequest {
  const windowSummary = fmtWindow(
    run.schedule.recommended_window.start,
    run.schedule.recommended_window.end,
  );

  const selectedResult: AiScenarioResult = {
    objective: run.input.objective,
    projected_cost: numericOrNull(run.metrics.optimized_cost),
    projected_emissions: numericOrNull(run.metrics.optimized_carbon_kg),
    schedule_summary: windowSummary ?? run.schedule.explanation ?? null,
  };

  const alternatives: AiScenarioResult[] = [];
  const baselineCost = numericOrNull(run.metrics.baseline_cost);
  const baselineCarbon = numericOrNull(run.metrics.baseline_carbon_kg);
  if (baselineCost !== null || baselineCarbon !== null) {
    alternatives.push({
      objective: "baseline",
      projected_cost: baselineCost,
      projected_emissions: baselineCarbon,
      schedule_summary: "Run immediately without optimization",
    });
  }

  return {
    selected_objective: run.input.objective,
    deadline: run.input.deadline ?? null,
    region: run.location.resolved_region ?? null,
    selected_result: selectedResult,
    alternatives,
  };
}

// ---------------------------------------------------------------------------
// Dedup key
// ---------------------------------------------------------------------------

function buildRunKey(run: OptimizeResponse): string {
  return [
    run.schedule.recommended_window.start ?? "none",
    run.schedule.recommended_window.end ?? "none",
    run.input.objective,
    String(run.schedule.recommended_window.selected_interval_count),
    String(run.input.compute_hours_required),
    run.location.resolved_region,
  ].join("|");
}

// ---------------------------------------------------------------------------
// Derive a single display text from the response.
// Prefers the new `summary` field; falls back to joining legacy sectioned
// fields so responses from older backend versions still render.
// ---------------------------------------------------------------------------

function resolveSummaryText(data: AiInterpretResponse): string | null {
  if (data.summary) return data.summary;

  // Legacy fallback: join non-null sectioned fields into one paragraph.
  const parts = [
    data.why_this_schedule,
    data.tradeoff_summary,
    data.scenario_comparison,
    data.recommendation_memo,
  ].filter(Boolean);

  return parts.length > 0 ? parts.join(" ") : null;
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function LoadingState() {
  return (
    <div className="rounded-[1.3rem] border border-violet-300/14 bg-[linear-gradient(180deg,rgba(167,139,250,0.06),rgba(167,139,250,0.01))] px-5 py-5">
      <div className="flex items-center gap-3">
        <span className="relative inline-flex h-4 w-4 shrink-0">
          <span className="absolute inset-0 animate-ping rounded-full bg-violet-300/40" />
          <span className="relative inline-flex h-4 w-4 rounded-full bg-violet-300/60" />
        </span>
        <p className="text-sm text-slate-100/50 animate-pulse">Generating AI summary of optimizer output…</p>
      </div>
    </div>
  );
}

function UnavailableState({ message }: { message?: string | null }) {
  return (
    <div className="rounded-[1.3rem] border border-dashed border-white/10 bg-black/20 px-5 py-4 text-sm text-muted text-center">
      {message ?? "AI summary unavailable for this run."}
    </div>
  );
}

function SummaryBubble({ text }: { text: string }) {
  return (
    <div className="rounded-[1.3rem] border border-violet-300/14 bg-[linear-gradient(180deg,rgba(167,139,250,0.08),rgba(167,139,250,0.02))] px-5 py-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
      <p className="text-xs uppercase tracking-[0.18em] text-violet-200/70 mb-3">AI Summary (interpreted by Claude)</p>
      <p className="text-sm leading-7 text-slate-100/90">{text}</p>
      <p className="mt-4 text-[11px] text-slate-100/35 leading-5">
        AI-generated summary based solely on optimizer output. Does not make scheduling decisions.
      </p>
    </div>
  );
}

function SuccessState({ data }: { data: AiInterpretResponse }) {
  const text = resolveSummaryText(data);
  if (!text) return <UnavailableState />;
  return <SummaryBubble text={text} />;
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

interface AiDecisionSummaryCardProps {
  optimizeResponse: OptimizeResponse;
}

export function AiDecisionSummaryCard({ optimizeResponse }: AiDecisionSummaryCardProps) {
  const runKey = buildRunKey(optimizeResponse);
  const lastKeyRef = useRef<string | null>(null);

  const interpretMutation = useMutation({
    mutationFn: interpretOptimization,
  });

  const mutateRef = useRef(interpretMutation.mutate);
  mutateRef.current = interpretMutation.mutate;

  useEffect(() => {
    if (runKey === lastKeyRef.current) return;
    lastKeyRef.current = runKey;
    mutateRef.current(buildInterpretPayload(optimizeResponse));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runKey]);

  const renderBody = () => {
    if (interpretMutation.isPending) return <LoadingState />;

    if (interpretMutation.isError) {
      return <UnavailableState message="AI summary unavailable for this run." />;
    }

    const data = interpretMutation.data;
    if (!data) return null;

    if (data.status === "unavailable" || data.status === "error") {
      return <UnavailableState message={data.message} />;
    }

    return <SuccessState data={data} />;
  };

  const body = renderBody();
  if (body === null) return null;

  return (
    <SectionCard
      title="AI Summary"
      eyebrow="AI Analysis"
      subtitle=""
      bodyClassName=""
    >
      {body}
    </SectionCard>
  );
}
