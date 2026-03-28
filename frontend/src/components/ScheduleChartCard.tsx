import type { TimeseriesRow } from "../types/api";

interface ScheduleChartCardProps {
  title: string;
  subtitle: string;
  rows: TimeseriesRow[];
}

function formatTickLabel(timestamp: string): string {
  const date = new Date(timestamp);
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  }).format(date);
}

export function ScheduleChartCard({ title, subtitle, rows }: ScheduleChartCardProps) {
  const visibleRows = rows.slice(0, 72);
  const runCount = visibleRows.filter((row) => row.run_flag).length;
  const eligibleCount = visibleRows.filter((row) => row.eligible_flag).length;

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-3 xl:flex-row xl:items-end xl:justify-between">
        <div>
          <h3 className="text-lg font-semibold text-text">{title}</h3>
          <p className="mt-1 text-sm leading-6 text-muted">{subtitle}</p>
        </div>
        <div className="flex flex-wrap gap-3 text-sm">
          <div className="rounded-full border border-border/80 bg-white/[0.03] px-3 py-2 text-slate-100/90">Run intervals <span className="ml-2 text-text">{runCount}</span></div>
          <div className="rounded-full border border-border/80 bg-white/[0.03] px-3 py-2 text-slate-100/90">Eligible intervals <span className="ml-2 text-text">{eligibleCount}</span></div>
        </div>
      </div>

      {!visibleRows.length ? (
        <div className="rounded-[1.6rem] border border-dashed border-border/80 bg-ink/30 p-8 text-sm text-muted">No schedule intervals are available yet.</div>
      ) : (
        <>
          <div className="overflow-hidden rounded-[1.6rem] border border-border/80 bg-[linear-gradient(180deg,rgba(8,12,22,0.92),rgba(13,19,32,0.9))] p-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.03),0_20px_40px_rgba(0,0,0,0.22)]">
            <div className="flex h-48 items-end gap-1.5">
              {visibleRows.map((row, index) => {
                const runHeight = row.run_flag ? 100 : 14;
                const eligibleHeight = row.eligible_flag ? 100 : 30;
                return (
                  <div key={`${row.timestamp}-${index}`} className="relative flex flex-1 items-end justify-center">
                    <div className="absolute bottom-0 w-full rounded-t-lg bg-white/7 shadow-[0_0_18px_rgba(255,255,255,0.04)]" style={{ height: `${eligibleHeight}%` }} />
                    <div className="relative z-10 w-full rounded-t-lg bg-gradient-to-t from-accent to-cyan-300 shadow-[0_0_22px_rgba(98,163,255,0.35)]" style={{ height: `${runHeight}%`, opacity: row.run_flag ? 1 : 0.22 }} />
                  </div>
                );
              })}
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-4 text-sm text-muted">
            <span className="inline-flex items-center gap-2 rounded-full border border-border/80 bg-white/[0.03] px-3 py-2">
              <span className="h-3 w-3 rounded-full bg-accent" />
              Run interval
            </span>
            <span className="inline-flex items-center gap-2 rounded-full border border-border/80 bg-white/[0.03] px-3 py-2">
              <span className="h-3 w-3 rounded-full bg-white/20" />
              Eligible interval
            </span>
            <span className="rounded-full border border-border/80 bg-white/[0.03] px-3 py-2">{formatTickLabel(visibleRows[0].timestamp)} to {formatTickLabel(visibleRows[visibleRows.length - 1].timestamp)}</span>
          </div>
        </>
      )}
    </div>
  );
}
