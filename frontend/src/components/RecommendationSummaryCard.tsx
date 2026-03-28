interface RecommendationSummaryCardProps {
  heading: string;
  recommendation: string;
  metadata: Array<{ label: string; value: string }>;
}

export function RecommendationSummaryCard({ heading, recommendation, metadata }: RecommendationSummaryCardProps) {
  return (
    <div className="rounded-[1.8rem] border border-emerald-300/14 bg-[linear-gradient(180deg,rgba(74,222,128,0.10),rgba(74,222,128,0.02))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.04),0_20px_36px_rgba(7,38,20,0.18)]">
      <p className="text-[11px] uppercase tracking-[0.2em] text-emerald-200">System Recommendation</p>
      <h3 className="mt-3 text-lg font-semibold text-text">{heading}</h3>
      <p className="mt-3 text-sm leading-7 text-slate-100/90">{recommendation}</p>
      <div className="mt-5 grid gap-3 sm:grid-cols-3">
        {metadata.map((item) => (
          <div key={item.label} className="rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3">
            <p className="text-[11px] uppercase tracking-[0.14em] text-muted">{item.label}</p>
            <p className="mt-2 text-sm font-medium text-text">{item.value}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
