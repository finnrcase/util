import { useState } from "react";
import { SectionCard } from "../../../components/SectionCard";
import type {
  FeasibilityAnalysis,
  FeasibilityBucket,
  FeasibilityDriver,
  OptimizeResponse,
  RecommendationCategory,
} from "../../../types/api";

interface OpportunityScreeningTabProps {
  data: OptimizeResponse | null;
}

function categoryColors(category: RecommendationCategory) {
  switch (category) {
    case "Prioritize":
      return {
        badge: "border-emerald-400/25 bg-emerald-400/10 text-emerald-200",
        dot: "bg-emerald-400",
        action: "border-emerald-400/20 bg-emerald-400/8 text-emerald-100",
      };
    case "Promising but monitor":
      return {
        badge: "border-sky-400/25 bg-sky-400/10 text-sky-200",
        dot: "bg-sky-400",
        action: "border-sky-400/20 bg-sky-400/8 text-sky-100",
      };
    case "Caution":
      return {
        badge: "border-amber-400/25 bg-amber-400/10 text-amber-200",
        dot: "bg-amber-400",
        action: "border-amber-400/20 bg-amber-400/8 text-amber-100",
      };
    case "Deprioritize":
      return {
        badge: "border-rose-400/25 bg-rose-400/10 text-rose-200",
        dot: "bg-rose-400",
        action: "border-rose-400/20 bg-rose-400/8 text-rose-100",
      };
  }
}

function riskScoreColor(score: number): string {
  if (score >= 65) return "bg-rose-500/70";
  if (score >= 35) return "bg-amber-400/70";
  return "bg-emerald-400/70";
}

function feasibilityScoreColor(score: number): string {
  if (score >= 65) return "bg-emerald-400/70";
  if (score >= 35) return "bg-amber-400/70";
  return "bg-rose-500/70";
}

function bucketBadgeClass(bucket: FeasibilityBucket, inverted = false): string {
  const effectiveBucket = inverted
    ? bucket === "High"
      ? "Low"
      : bucket === "Low"
        ? "High"
        : "Moderate"
    : bucket;

  switch (effectiveBucket) {
    case "High":
      return "border-rose-400/20 bg-rose-400/10 text-rose-200";
    case "Moderate":
      return "border-amber-400/20 bg-amber-400/10 text-amber-200";
    case "Low":
      return "border-emerald-400/20 bg-emerald-400/10 text-emerald-200";
  }
}

function driverBadgeClass(direction: "risk" | "opportunity") {
  return direction === "opportunity"
    ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-200"
    : "border-amber-400/20 bg-amber-400/10 text-amber-200";
}

function isFeasibilityAnalysis(value: OptimizeResponse["feasibility_analysis"]): value is FeasibilityAnalysis {
  if (!value) return false;
  return Boolean(
    value.recommendation &&
      value.summary &&
      value.component_scores &&
      Array.isArray(value.drivers) &&
      typeof value.interpretation === "string",
  );
}

function formatScore(score: number | null | undefined): string {
  return typeof score === "number" ? score.toFixed(0) : "--";
}

function scoreWidth(score: number | null | undefined): string {
  const value = typeof score === "number" ? score : 0;
  return `${Math.min(Math.max(value, 0), 100)}%`;
}

function ScoreCard({
  title,
  score,
  bucket,
  inverted = false,
  supporting,
}: {
  title: string;
  score: number | null | undefined;
  bucket: FeasibilityBucket;
  inverted?: boolean;
  supporting?: string;
}) {
  const numericScore = typeof score === "number" ? score : 0;
  const barColor = inverted ? feasibilityScoreColor(numericScore) : riskScoreColor(numericScore);
  const bucketClass = bucketBadgeClass(bucket, inverted);

  return (
    <div className="relative overflow-hidden rounded-[1.6rem] border border-border/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.04),rgba(255,255,255,0.02))] p-5 shadow-[0_18px_40px_rgba(0,0,0,0.18)]">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.06),transparent_28%)]" />
      <div className="relative">
        <p className="text-xs uppercase tracking-[0.18em] text-muted">{title}</p>
        <p className="mt-4 text-3xl font-semibold tracking-[-0.03em] text-text">
          {formatScore(score)}
          <span className="ml-1 text-base font-normal text-muted">/100</span>
        </p>
        <div className="mt-3 h-1.5 w-full rounded-full bg-white/[0.06]">
          <div className={`h-1.5 rounded-full transition-all ${barColor}`} style={{ width: scoreWidth(score) }} />
        </div>
        <span className={`mt-3 inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs ${bucketClass}`}>
          {bucket}
        </span>
        {supporting ? <p className="mt-2 text-xs text-muted">{supporting}</p> : null}
      </div>
    </div>
  );
}

function RecommendationBadgeCard({ category, headline }: { category: RecommendationCategory; headline: string }) {
  const colors = categoryColors(category);

  return (
    <div className="relative overflow-hidden rounded-[1.6rem] border border-border/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.04),rgba(255,255,255,0.02))] p-5 shadow-[0_18px_40px_rgba(0,0,0,0.18)]">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.06),transparent_28%)]" />
      <div className="relative">
        <p className="text-xs uppercase tracking-[0.18em] text-muted">Recommendation</p>
        <div className={`mt-4 inline-flex items-center gap-2 rounded-full border px-3 py-1.5 text-sm font-medium ${colors.badge}`}>
          <span className={`h-2 w-2 rounded-full ${colors.dot}`} />
          {category}
        </div>
        <p className="mt-3 text-sm leading-6 text-muted">{headline}</p>
      </div>
    </div>
  );
}

function ComponentScoreRow({
  label,
  score,
}: {
  label: string;
  score: number | null | undefined;
}) {
  const numericScore = typeof score === "number" ? score : 0;

  return (
    <div className="flex items-center gap-4">
      <span className="w-40 shrink-0 text-sm text-muted">{label}</span>
      <div className="h-2 flex-1 rounded-full bg-white/[0.06]">
        <div className={`h-2 rounded-full transition-all ${riskScoreColor(numericScore)}`} style={{ width: scoreWidth(score) }} />
      </div>
      <span className="w-10 shrink-0 text-right text-sm tabular-nums text-slate-300">{formatScore(score)}</span>
    </div>
  );
}

function DriverCard({ driver }: { driver: FeasibilityDriver }) {
  return (
    <div className="rounded-[1.2rem] border border-border/80 bg-white/[0.03] p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2.5">
          <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-white/[0.06] text-xs font-semibold text-slate-300">
            {driver.rank}
          </span>
          <span className="text-sm font-medium text-text">{driver.label}</span>
        </div>
        <span className={`shrink-0 rounded-full border px-2 py-0.5 text-xs ${driverBadgeClass(driver.direction)}`}>
          {driver.direction === "opportunity" ? "Opportunity" : "Risk"}
        </span>
      </div>
      <p className="mt-2.5 pl-8 text-sm leading-6 text-muted">{driver.detail}</p>
    </div>
  );
}

function EmptyState() {
  return (
    <SectionCard
      eyebrow="Opportunity Screening"
      title="Scheduling Feasibility"
      subtitle="Run the optimizer to generate an opportunity screening analysis for this workload."
    >
      <div className="rounded-[1.6rem] border border-dashed border-white/10 bg-black/20 p-10 text-center text-muted">
        No run available yet. Use the Optimizer tab to generate Opportunity Screening.
      </div>
    </SectionCard>
  );
}

function UnavailableState() {
  return (
    <SectionCard
      eyebrow="Opportunity Screening"
      title="Scheduling Feasibility"
      subtitle="The latest optimizer run did not include screening data."
    >
      <div className="rounded-[1.6rem] border border-dashed border-white/10 bg-black/20 p-10 text-center text-muted">
        The optimizer result is still available. Opportunity Screening was omitted or incomplete for this run.
      </div>
    </SectionCard>
  );
}

export function OpportunityScreeningTab({ data }: OpportunityScreeningTabProps) {
  const [showComponents, setShowComponents] = useState(false);

  if (!data) return <EmptyState />;
  if (!isFeasibilityAnalysis(data.feasibility_analysis)) return <UnavailableState />;

  const { recommendation, summary, component_scores, drivers, interpretation } = data.feasibility_analysis;
  const colors = categoryColors(recommendation.category);

  return (
    <div className="flex flex-col gap-6">
      <SectionCard
        eyebrow="Opportunity Screening"
        title="Scheduling Feasibility"
        subtitle="How suitable this workload is for optimized scheduling under current grid and deadline conditions."
      >
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <ScoreCard
            title="Feasibility"
            score={summary.feasibility_score}
            bucket={summary.feasibility_bucket}
            inverted
            supporting={summary.overall_label}
          />
          <ScoreCard
            title="Infrastructure Friction"
            score={summary.friction_score}
            bucket={summary.friction_bucket}
            supporting="Lower is better"
          />
          <ScoreCard
            title="Delay Risk"
            score={summary.delay_risk_score}
            bucket={summary.delay_risk_bucket}
            supporting="Risk of waiting"
          />
          <RecommendationBadgeCard category={recommendation.category} headline={recommendation.headline} />
        </div>
      </SectionCard>

      <SectionCard eyebrow="Recommendation" title={recommendation.headline}>
        <div className="flex flex-col gap-4">
          <div>
            <span className={`inline-flex items-center gap-2 rounded-full border px-3.5 py-1.5 text-sm font-medium ${colors.badge}`}>
              <span className={`h-2 w-2 rounded-full ${colors.dot}`} />
              {recommendation.category}
            </span>
          </div>
          <p className="max-w-3xl text-sm leading-7 text-slate-200">{recommendation.body}</p>
          <div className={`mt-1 rounded-[1.1rem] border px-4 py-3 text-sm leading-6 ${colors.action}`}>
            <span className="mr-2 font-semibold uppercase tracking-[0.14em] opacity-70">Next step</span>
            {recommendation.action}
          </div>
        </div>
      </SectionCard>

      {drivers.length > 0 ? (
        <SectionCard
          eyebrow="Analysis"
          title="Primary Drivers"
          subtitle="The factors with the most influence on this screening result."
        >
          <div className="flex flex-col gap-3">
            {drivers.map((driver) => (
              <DriverCard key={driver.key} driver={driver} />
            ))}
          </div>
        </SectionCard>
      ) : null}

      <SectionCard eyebrow="Context" title="Interpretation">
        <div className="max-w-3xl">
          <p className="text-sm leading-7 text-slate-200">{interpretation}</p>
        </div>
      </SectionCard>

      <SectionCard
        eyebrow="Scoring Detail"
        title="Component Scores"
        subtitle="Individual risk dimensions that feed into the combined scores above."
        action={(
          <button
            type="button"
            onClick={() => setShowComponents((value) => !value)}
            className="shrink-0 rounded-full border border-white/10 bg-white/[0.04] px-3 py-1.5 text-xs text-muted transition hover:border-white/20 hover:bg-white/[0.07]"
          >
            {showComponents ? "Hide" : "Show"}
          </button>
        )}
      >
        {showComponents ? (
          <div className="flex max-w-lg flex-col gap-4">
            <ComponentScoreRow label="Grid Stress" score={component_scores.grid_stress_score} />
            <ComponentScoreRow label="Price Volatility" score={component_scores.price_volatility_risk} />
            <ComponentScoreRow label="Carbon Instability" score={component_scores.carbon_instability_risk} />
            <ComponentScoreRow label="Timing Risk" score={component_scores.timing_risk} />
            <ComponentScoreRow label="Load Pressure" score={component_scores.load_pressure_score} />
            <p className="mt-1 text-xs text-muted">
              All scores are 0-100. Higher means more risk on that dimension.
            </p>
          </div>
        ) : (
          <p className="text-sm text-muted">Click Show to expand the component breakdown.</p>
        )}
      </SectionCard>
    </div>
  );
}
