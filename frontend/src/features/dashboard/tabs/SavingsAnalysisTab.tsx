import { SectionCard } from "../../../components/SectionCard";
import { SummaryCard } from "../../../components/SummaryCard";
import { StatusBadge } from "../../../components/StatusBadge";
import { RecommendationSummaryCard } from "../../../components/RecommendationSummaryCard";
import { ComparisonBarChart } from "../../../components/ComparisonBarChart";
import { DeltaImpactChart } from "../../../components/DeltaImpactChart";
import type { OptimizeResponse, SummaryCardModel } from "../../../types/api";
import { compactRuntimeLabel, completionStatus, formatDateTime, formatKg, recommendationText } from "../utils";

interface SavingsAnalysisTabProps {
  data?: OptimizeResponse;
  isLoading: boolean;
  errorMessage?: string;
}

function formatCurrency(value: number | null | undefined): string {
  return `$${Number(value ?? 0).toFixed(2)}`;
}

export function SavingsAnalysisTab({ data, isLoading, errorMessage }: SavingsAnalysisTabProps) {
  if (isLoading) {
    return <SectionCard title="Savings Analysis" subtitle=""><div className="rounded-[1.6rem] border border-white/10 bg-white/[0.03] p-10 text-center text-muted">Calculating savings, carbon outcomes, and the recommendation summary...</div></SectionCard>;
  }

  if (errorMessage) {
    return <SectionCard title="Savings Analysis" subtitle=""><div className="rounded-[1.6rem] border border-danger/25 bg-danger/10 p-6 text-sm text-red-100">{errorMessage}</div></SectionCard>;
  }

  if (!data) {
    return <SectionCard title="Savings Analysis" subtitle=""><div className="rounded-[1.6rem] border border-dashed border-white/10 bg-black/20 p-10 text-center text-muted">No run available yet. Use the Optimizer tab to generate savings and impact outputs.</div></SectionCard>;
  }

  const workloadEnergyKwh = (data.input.compute_hours_required * data.input.machine_watts) / 1000;
  const baselineCost = Number(data.metrics.baseline_cost ?? 0);
  const optimizedCost = Number(data.metrics.optimized_cost ?? 0);
  const baselineCarbon = Number(data.metrics.baseline_carbon_kg ?? 0);
  const optimizedCarbon = Number(data.metrics.optimized_carbon_kg ?? 0);
  const costSavings = Number(data.metrics.cost_savings ?? 0);
  const carbonSavings = Number(data.metrics.carbon_savings_kg ?? 0);
  const costReductionPct = Number(data.metrics.cost_reduction_pct ?? 0);
  const carbonReductionPct = Number(data.metrics.carbon_reduction_pct ?? 0);
  const objectiveLabel = (data.input.objective ?? "balanced").replace(/_/g, " ");

  const cards: SummaryCardModel[] = [
    { id: "workload_energy", title: "Workload Energy", value: `${workloadEnergyKwh.toFixed(2)} kWh` },
    { id: "cost_outcome", title: "Cost Outcome", value: formatCurrency(optimizedCost), supporting_text: `Baseline ${formatCurrency(baselineCost)}`, tone: "positive" },
    { id: "carbon_outcome", title: "Carbon Outcome", value: formatKg(optimizedCarbon), supporting_text: `Baseline ${formatKg(baselineCarbon)}`, tone: "positive" },
    { id: "saved_vs_baseline", title: "Savings vs Baseline", value: formatCurrency(costSavings), supporting_text: `${costReductionPct.toFixed(1)}% lower`, tone: "positive" },
    { id: "carbon_reduction", title: "Carbon Reduction", value: formatKg(carbonSavings), supporting_text: `${carbonReductionPct.toFixed(1)}% lower`, tone: "positive" },
    { id: "deadline_status", title: "Deadline / Completion", value: completionStatus(data), supporting_text: formatDateTime(data.input.deadline) },
  ];

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-2 2xl:grid-cols-3">
        {cards.map((card) => (
          <SummaryCard key={card.id} {...card} />
        ))}
      </div>

      <SectionCard title="Baseline vs Optimized" subtitle="" eyebrow="Comparison" bodyClassName="space-y-6">
        <div className="grid gap-6 xl:grid-cols-2">
          <ComparisonBarChart
            title="Electricity Cost Comparison"
            subtitle="Lower cost indicates a cheaper schedule across the same workload and deadline."
            baselineLabel="Baseline Cost"
            optimizedLabel="Optimized Cost"
            baselineValue={baselineCost}
            optimizedValue={optimizedCost}
            unitLabel="USD"
            baselineColor="#7c8598"
            optimizedColor="#8b5cf6"
          />
          <ComparisonBarChart
            title="Carbon Emissions Comparison"
            subtitle="Lower carbon indicates reduced estimated emissions for the same run."
            baselineLabel="Baseline Carbon"
            optimizedLabel="Optimized Carbon"
            baselineValue={baselineCarbon}
            optimizedValue={optimizedCarbon}
            unitLabel="kg CO2"
            baselineColor="#7c8598"
            optimizedColor="#2dd4bf"
          />
        </div>
      </SectionCard>

      <SectionCard title="Tradeoff Outcomes" subtitle="" eyebrow="Tradeoff" bodyClassName="space-y-6">
        <div className="grid gap-6 xl:grid-cols-[minmax(0,1.3fr)_360px]">
          <div className="grid gap-6 md:grid-cols-2">
            <ComparisonBarChart
              title="Observed Cost Under Current Objective"
              subtitle="Compares baseline cost with the schedule selected under the current objective."
              baselineLabel="Baseline Cost"
              optimizedLabel="Chosen Schedule Cost"
              baselineValue={baselineCost}
              optimizedValue={optimizedCost}
              unitLabel="USD"
              baselineColor="#6b7280"
              optimizedColor="#8b5cf6"
            />
            <ComparisonBarChart
              title="Observed Carbon Under Current Objective"
              subtitle="Compares baseline carbon with the same chosen schedule."
              baselineLabel="Baseline Carbon"
              optimizedLabel="Chosen Schedule Carbon"
              baselineValue={baselineCarbon}
              optimizedValue={optimizedCarbon}
              unitLabel="kg CO2"
              baselineColor="#6b7280"
              optimizedColor="#22c55e"
            />
          </div>
          <RecommendationSummaryCard
            heading="Recommendation Summary"
            recommendation={recommendationText(data)}
            metadata={[
              { label: "Run Window", value: `${formatDateTime(data.schedule.recommended_window.start)} to ${formatDateTime(data.schedule.recommended_window.end)}` },
              { label: "Pricing Source", value: `${data.pricing.pricing_source} ${data.pricing.pricing_market_label}`.trim() },
              { label: "Carbon Source", value: data.forecast.carbon_signal_mix.join(", ") || "Forecast" },
            ]}
          />
        </div>
      </SectionCard>

      <SectionCard title="Cost and Carbon Impact" subtitle="" eyebrow="Impact" bodyClassName="space-y-6">
        <div className="grid gap-6 xl:grid-cols-2">
          <DeltaImpactChart
            title="Absolute Improvement vs Baseline"
            subtitle="Bigger bars indicate larger reductions delivered by the optimized schedule."
            items={[
              { label: "Cost Savings", value: costSavings, displayValue: formatCurrency(costSavings), positiveIsGood: true, tone: "cost" },
              { label: "Carbon Reduction", value: carbonSavings, displayValue: formatKg(carbonSavings), positiveIsGood: true, tone: "carbon" },
            ]}
          />
          <DeltaImpactChart
            title="Percent Improvement vs Baseline"
            subtitle="Shows the relative reduction in cost and carbon compared with the baseline plan."
            items={[
              { label: "Cost Reduction", value: costReductionPct, displayValue: `${costReductionPct.toFixed(1)}%`, positiveIsGood: true, tone: "cost" },
              { label: "Carbon Reduction", value: carbonReductionPct, displayValue: `${carbonReductionPct.toFixed(1)}%`, positiveIsGood: true, tone: "carbon" },
            ]}
          />
        </div>
      </SectionCard>

      <SectionCard title="Current Run Signals" subtitle="" eyebrow="Context" bodyClassName="space-y-5">
        <div className="flex flex-wrap gap-3">
          {data.summary.badges.map((badge) => (
            <StatusBadge key={badge.id} {...badge} />
          ))}
        </div>
        <div className="grid gap-4 md:grid-cols-2">
          {data.charts.baseline_vs_optimized_comparison.rows.map((row) => (
            <div key={row.metric} className="rounded-[1.4rem] border border-white/10 bg-white/[0.04] p-4">
              <p className="text-[11px] uppercase tracking-[0.16em] text-muted">{row.metric}</p>
              <p className="mt-2 text-sm text-muted">Baseline {row.baseline} {row.unit}</p>
              <p className="mt-1 text-sm text-emerald-100">Optimized {row.optimized} {row.unit}</p>
            </div>
          ))}
        </div>
      </SectionCard>
    </div>
  );
}







