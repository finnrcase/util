import { SectionCard } from "./SectionCard";
import { SimpleLineChart } from "./SimpleLineChart";
import type { ChartPoint, ForecastStatItem } from "../features/dashboard/utils";

interface ForecastChartCardProps {
  eyebrow: string;
  title: string;
  subtitle: string;
  rows: ChartPoint[];
  stroke: string;
  unitLabel: string;
  markerTimestamps: string[];
  stats: ForecastStatItem[];
  interpretation: string;
}

export function ForecastChartCard({ eyebrow, title, subtitle, rows, stroke, unitLabel, markerTimestamps, stats, interpretation }: ForecastChartCardProps) {
  return (
    <SectionCard title={title} subtitle={subtitle} eyebrow={eyebrow} bodyClassName="space-y-6">
      <div className="grid gap-4 lg:grid-cols-3 xl:grid-cols-6">
        {stats.map((stat) => (
          <div key={stat.label} className="rounded-[1.3rem] border border-white/10 bg-white/[0.04] px-4 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
            <p className="text-[11px] uppercase tracking-[0.16em] text-muted">{stat.label}</p>
            <p className="mt-2 text-sm font-medium leading-6 text-text">{stat.value}</p>
          </div>
        ))}
      </div>

      <SimpleLineChart rows={rows} stroke={stroke} unitLabel={unitLabel} markerTimestamps={markerTimestamps} markerColor="#4ade80" markerLabel="Recommended run interval" />

      <div className="rounded-[1.35rem] border border-white/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.04),rgba(255,255,255,0.02))] px-4 py-3 text-sm leading-6 text-slate-100/90">
        {interpretation}
      </div>
    </SectionCard>
  );
}
