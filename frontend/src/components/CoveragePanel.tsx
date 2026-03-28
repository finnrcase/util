import type { CoverageResponse } from "../types/api";

interface CoveragePanelProps {
  coverage?: CoverageResponse;
  isLoading: boolean;
  errorMessage?: string;
}

export function CoveragePanel({ coverage, isLoading, errorMessage }: CoveragePanelProps) {
  if (isLoading) {
    return <div className="rounded-[1.6rem] border border-border/80 bg-white/[0.03] p-5 text-sm text-muted">Loading supported market coverage...</div>;
  }

  if (errorMessage) {
    return <div className="rounded-[1.6rem] border border-amber-400/20 bg-amber-400/10 p-5 text-sm text-amber-100">{errorMessage}</div>;
  }

  if (!coverage) {
    return <div className="rounded-[1.6rem] border border-dashed border-border/80 p-5 text-sm text-muted">Coverage information is not available yet.</div>;
  }

  return (
    <div className="space-y-5">
      <div className="rounded-[1.6rem] border border-border/80 bg-white/[0.03] p-5">
        <p className="text-sm leading-7 text-muted">{coverage.summary}</p>
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        {coverage.supported_live_markets.map((market) => (
          <div key={market.market} className="rounded-[1.6rem] border border-border/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.04),rgba(255,255,255,0.02))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
            <div className="flex items-center justify-between gap-3">
              <h3 className="text-base font-semibold text-text">{market.market}</h3>
              <span className="rounded-full border border-emerald-400/20 bg-emerald-400/10 px-3 py-1 text-xs uppercase tracking-[0.14em] text-emerald-200">
                {market.status}
              </span>
            </div>
            <p className="mt-3 text-sm leading-6 text-muted">{market.coverage}</p>
            <p className="mt-4 text-sm text-slate-100/90">Examples: <span className="text-text">{market.examples.join(", ")}</span></p>
          </div>
        ))}
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <div className="rounded-[1.6rem] border border-border/80 bg-white/[0.03] p-5">
          <p className="text-[11px] uppercase tracking-[0.18em] text-accent">Partial Support Notes</p>
          <div className="mt-4 space-y-3">
            {coverage.partially_supported_notes.map((note) => (
              <p key={`${note.market}-${note.note}`} className="text-sm leading-6 text-slate-100/90">
                <span className="font-medium text-text">{note.market}:</span> {note.note}
              </p>
            ))}
          </div>
        </div>

        <div className="rounded-[1.6rem] border border-border/80 bg-white/[0.03] p-5">
          <p className="text-[11px] uppercase tracking-[0.18em] text-accent">Fallback Behavior</p>
          <p className="mt-3 text-sm font-medium text-text">{coverage.unsupported_behavior.label}</p>
          <p className="mt-2 text-sm leading-6 text-muted">{coverage.unsupported_behavior.message}</p>
        </div>
      </div>
    </div>
  );
}
