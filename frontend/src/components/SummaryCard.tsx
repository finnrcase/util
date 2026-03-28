import type { SummaryCardModel } from "../types/api";

const toneClassMap: Record<string, string> = {
  positive: "border-emerald-400/20 bg-[linear-gradient(180deg,rgba(16,185,129,0.08),rgba(16,185,129,0.02))]",
  warning: "border-amber-400/20 bg-[linear-gradient(180deg,rgba(245,158,11,0.08),rgba(245,158,11,0.02))]",
  default: "border-border/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.04),rgba(255,255,255,0.02))]"
};

export function SummaryCard({ title, value, supporting_text, tone = "default" }: SummaryCardModel) {
  return (
    <div className={`relative overflow-hidden rounded-[1.6rem] border p-5 shadow-[0_18px_40px_rgba(0,0,0,0.18)] ${toneClassMap[tone] ?? toneClassMap.default}`}>
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.06),transparent_28%)]" />
      <div className="relative">
        <p className="text-xs uppercase tracking-[0.18em] text-muted">{title}</p>
        <p className="mt-4 text-3xl font-semibold tracking-[-0.03em] text-text">{value ?? "--"}</p>
        {supporting_text ? <p className="mt-3 text-sm leading-6 text-muted">{supporting_text}</p> : null}
      </div>
    </div>
  );
}
