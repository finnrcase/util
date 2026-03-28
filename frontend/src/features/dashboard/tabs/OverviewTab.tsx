import { SectionCard } from "../../../components/SectionCard";
import { SummaryCard } from "../../../components/SummaryCard";
import { StatusBadge } from "../../../components/StatusBadge";
import { RecommendationSummaryCard } from "../../../components/RecommendationSummaryCard";
import { PageHero } from "../../../components/PageHero";
import type { OptimizeResponse, SummaryCardModel } from "../../../types/api";
import { compactRuntimeLabel, completionStatus, formatDateTime, recommendationText } from "../utils";

interface OverviewTabProps {
  data?: OptimizeResponse;
  isLoading: boolean;
  errorMessage?: string;
  apiMode: string;
}

export function OverviewTab({ data, isLoading, errorMessage, apiMode }: OverviewTabProps) {
  if (isLoading) {
    return <SectionCard title="Overview" subtitle="Preparing the optimization summary."><div className="rounded-[1.6rem] border border-white/10 bg-white/[0.03] p-10 text-center text-muted">Running the optimization engine and building the dashboard summary...</div></SectionCard>;
  }

  if (errorMessage) {
    return <SectionCard title="Overview" subtitle="The latest optimize request returned an error."><div className="rounded-[1.6rem] border border-danger/25 bg-danger/10 p-6 text-sm text-red-100">{errorMessage}</div></SectionCard>;
  }

  if (!data) {
    return (
      <div className="space-y-6">
        <PageHero
          eyebrow="Overview"
          title="Executive summary for every optimization run"
          description=""
          meta={[
            { label: "Mode", value: apiMode },
            { label: "Live Pricing", value: "CAISO + ERCOT" },
            { label: "Structure", value: "Multi-tab dashboard" },
          ]}
        />
        <SectionCard title="Awaiting Run" subtitle="">
          <div className="rounded-[1.6rem] border border-dashed border-white/10 bg-black/20 p-10 text-center text-muted">No optimization results yet. Head to the Optimizer tab to create a scenario and run the engine.</div>
        </SectionCard>
      </div>
    );
  }

  const topCards: SummaryCardModel[] = [
    data.summary.cards.find((card) => card.id === "baseline_cost") ?? { id: "baseline_cost", title: "Estimated Cost", value: "--" },
    data.summary.cards.find((card) => card.id === "optimized_cost") ?? { id: "optimized_cost", title: "Optimized Cost", value: "--", tone: "positive" },
    data.summary.cards.find((card) => card.id === "cost_savings") ?? { id: "cost_savings", title: "Estimated Savings", value: "--", tone: "positive" },
    data.summary.cards.find((card) => card.id === "carbon_reduction") ?? { id: "carbon_reduction", title: "Carbon Reduction", value: "--", tone: "positive" },
    { id: "runtime", title: "Runtime", value: compactRuntimeLabel(data), supporting_text: "Requested compute duration" },
    { id: "deadline", title: "Completion Status", value: completionStatus(data), supporting_text: formatDateTime(data.input.deadline) },
  ];

  const recommendationMeta = [
    { label: "Run Window", value: `${formatDateTime(data.schedule.recommended_window.start)} to ${formatDateTime(data.schedule.recommended_window.end)}` },
    { label: "Pricing Source", value: `${data.pricing.pricing_source} ${data.pricing.pricing_market_label}`.trim() },
    { label: "Carbon Source", value: data.forecast.carbon_signal_mix.join(", ") || "Forecast" },
  ];

  return (
    <div className="space-y-6">
      <PageHero
        eyebrow="Overview"
        title="Operator summary for the current optimization run"
        description=""
        meta={[
          { label: "ZIP", value: data.input.zip_code },
          { label: "Region", value: data.location.resolved_region || "Pending" },
          { label: "Pricing", value: data.pricing.pricing_status_label },
          { label: "Deadline", value: formatDateTime(data.input.deadline) },
        ]}
      />

      <div className="grid gap-4 md:grid-cols-2 2xl:grid-cols-3">
        {topCards.map((card) => (
          <SummaryCard key={card.id} {...card} />
        ))}
      </div>

      <SectionCard title="Run Recommendation" subtitle="" eyebrow="Recommendation" bodyClassName="space-y-5">
        <div className="flex flex-wrap gap-3">
          {data.summary.badges.map((badge) => (
            <StatusBadge key={badge.id} {...badge} />
          ))}
        </div>
        <RecommendationSummaryCard
          heading="Recommended execution window"
          recommendation={recommendationText(data)}
          metadata={recommendationMeta}
        />
      </SectionCard>
    </div>
  );
}




