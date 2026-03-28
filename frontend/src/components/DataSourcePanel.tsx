import type { OptimizeResponse } from "../types/api";

interface DataSourcePanelProps {
  provenance: OptimizeResponse["provenance"];
  pricing: OptimizeResponse["pricing"];
}

function formatList(values: string[]): string {
  return values.length ? values.join(", ") : "--";
}

function formatObjective(value: string): string {
  return value.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

const rows = [
  { label: "ZIP Entered", key: "zip_code" },
  { label: "Resolved Region", key: "resolved_region" },
  { label: "Location Lookup", key: "location_lookup_status" },
  { label: "Pricing Status", key: "pricing_status" },
  { label: "Price Provider", key: "pricing_source" },
  { label: "Market Type", key: "pricing_market" },
  { label: "Node / Zone", key: "pricing_node" }
] as const;

export function DataSourcePanel({ provenance, pricing }: DataSourcePanelProps) {
  return (
    <div className="grid gap-5 xl:grid-cols-[1.15fr_0.85fr]">
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        {rows.map((row) => (
          <div key={row.key} className="rounded-[1.4rem] border border-border/80 bg-white/[0.03] p-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
            <p className="text-[11px] uppercase tracking-[0.16em] text-muted">{row.label}</p>
            <p className="mt-3 text-sm font-medium leading-6 text-text">{String(provenance[row.key] ?? pricing[row.key as keyof typeof pricing] ?? "--")}</p>
          </div>
        ))}
      </div>
      <div className="rounded-[1.6rem] border border-border/80 bg-[linear-gradient(180deg,rgba(255,255,255,0.04),rgba(255,255,255,0.02))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)]">
        <p className="text-[11px] uppercase tracking-[0.18em] text-accent">Run Context</p>
        <dl className="mt-4 space-y-4 text-sm">
          <div>
            <dt className="text-muted">Price Signal Source</dt>
            <dd className="mt-1 text-text">{formatList(provenance.price_signal_source)}</dd>
          </div>
          <div>
            <dt className="text-muted">Carbon Source</dt>
            <dd className="mt-1 text-text">{formatList(provenance.carbon_source)}</dd>
          </div>
          <div>
            <dt className="text-muted">Objective</dt>
            <dd className="mt-1 text-text">{formatObjective(provenance.objective)}</dd>
          </div>
          <div>
            <dt className="text-muted">Coverage Note</dt>
            <dd className="mt-1 leading-6 text-text">{provenance.coverage_note || pricing.pricing_message || "Live pricing and carbon sourcing are active for this run."}</dd>
          </div>
        </dl>
      </div>
    </div>
  );
}
