import type { StatusBadgeModel } from "../types/api";

const toneClassMap: Record<string, string> = {
  positive: "border-emerald-400/20 bg-emerald-400/10 text-emerald-100",
  warning: "border-amber-400/20 bg-amber-400/10 text-amber-100",
  neutral: "border-border/80 bg-white/[0.04] text-slate-100"
};

export function StatusBadge({ label, value, tone }: StatusBadgeModel) {
  return (
    <span className={`inline-flex items-center gap-2 rounded-full border px-3.5 py-2 text-sm shadow-[inset_0_1px_0_rgba(255,255,255,0.03)] ${toneClassMap[tone] ?? toneClassMap.neutral}`}>
      <span className="h-2 w-2 rounded-full bg-current opacity-80" />
      <span className="text-muted">{label}</span>
      <span className="font-medium text-current">{value}</span>
    </span>
  );
}
