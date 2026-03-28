import { useMemo, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { SectionCard } from "../../../components/SectionCard";
import { StatusBadge } from "../../../components/StatusBadge";
import { MultiSeriesLineChart } from "../../../components/MultiSeriesLineChart";
import { optimizeScenario } from "../../../lib/api";
import type { FormValues } from "../form";
import { chartRowsFromCarbon, chartRowsFromPrice, formatDateTime, formatKg } from "../utils";
import type { Objective, OptimizeRequest, OptimizeResponse } from "../../../types/api";

interface MultiLocationTabProps {
  initialValues: FormValues;
  onUseBestLocation: (zip: string) => void;
}

interface LocationResult {
  slotId: "A" | "B" | "C";
  zip: string;
  result?: OptimizeResponse;
  error?: string;
}

const slotDefinitions: Array<{ id: "A" | "B" | "C"; label: string }> = [
  { id: "A", label: "Location A" },
  { id: "B", label: "Location B" },
  { id: "C", label: "Location C" },
];

const comparisonStrokes = ["#8b5cf6", "#22d3ee", "#4ade80"] as const;

function fieldClass() {
  return "w-full rounded-[1.15rem] border border-white/10 bg-[#0d1220]/88 px-4 py-3 text-slate-100 placeholder:text-slate-400/70 outline-none transition shadow-[inset_0_1px_0_rgba(255,255,255,0.03)] focus:border-violet-300/45 focus:bg-[#10172a] focus:shadow-[0_0_0_3px_rgba(139,92,246,0.14)]";
}

function selectClass() {
  return `${fieldClass()} appearance-none bg-[linear-gradient(180deg,rgba(15,22,37,0.96),rgba(10,14,25,0.96))] pr-11`;
}

function formatCurrency(value: number | null | undefined): string {
  return `$${Number(value ?? 0).toFixed(2)}`;
}

function formatObjectiveLabel(objective: Objective): string {
  if (objective === "cost") {
    return "Cost";
  }
  if (objective === "carbon") {
    return "Carbon";
  }
  return "Balanced";
}

function buildOptimizePayload(values: FormValues, zip: string): OptimizeRequest {
  return {
    zip_code: zip,
    compute_hours_required: values.compute_hours_required,
    deadline: values.deadline,
    objective: values.objective,
    machine_watts: values.machine_watts,
    forecast_mode: "live_carbon",
    schedule_mode: values.schedule_mode,
    carbon_estimation_mode: values.carbon_estimation_mode,
    historical_days: values.historical_days,
  };
}

function validateZips(zips: string[]): string | null {
  if (zips.some((zip) => !zip.trim())) {
    return "Enter all three ZIP codes before running the comparison.";
  }

  if (zips.some((zip) => !/^\d{5}$/.test(zip.trim()))) {
    return "Each location must use a valid 5-digit ZIP code.";
  }

  const normalized = zips.map((zip) => zip.trim());
  if (new Set(normalized).size !== normalized.length) {
    return "Use three unique ZIP codes for the comparison.";
  }

  return null;
}

function pickBestLocation(results: LocationResult[], objective: Objective): LocationResult | undefined {
  const successful = results.filter((entry): entry is LocationResult & { result: OptimizeResponse } => Boolean(entry.result));
  if (!successful.length) {
    return undefined;
  }

  if (objective === "cost") {
    return successful.reduce((best, current) => (Number(current.result.metrics.optimized_cost ?? Number.POSITIVE_INFINITY) < Number(best.result.metrics.optimized_cost ?? Number.POSITIVE_INFINITY) ? current : best));
  }

  if (objective === "carbon") {
    return successful.reduce((best, current) => (Number(current.result.metrics.optimized_carbon_kg ?? Number.POSITIVE_INFINITY) < Number(best.result.metrics.optimized_carbon_kg ?? Number.POSITIVE_INFINITY) ? current : best));
  }

  const costValues = successful.map((entry) => Number(entry.result.metrics.optimized_cost ?? 0));
  const carbonValues = successful.map((entry) => Number(entry.result.metrics.optimized_carbon_kg ?? 0));
  const minCost = Math.min(...costValues);
  const maxCost = Math.max(...costValues);
  const minCarbon = Math.min(...carbonValues);
  const maxCarbon = Math.max(...carbonValues);

  const normalize = (value: number, min: number, max: number) => (max === min ? 0 : (value - min) / (max - min));

  return successful.reduce((best, current) => {
    const currentScore = normalize(Number(current.result.metrics.optimized_cost ?? 0), minCost, maxCost) + normalize(Number(current.result.metrics.optimized_carbon_kg ?? 0), minCarbon, maxCarbon);
    const bestScore = normalize(Number(best.result.metrics.optimized_cost ?? 0), minCost, maxCost) + normalize(Number(best.result.metrics.optimized_carbon_kg ?? 0), minCarbon, maxCarbon);
    return currentScore < bestScore ? current : best;
  });
}

function LocationCard({
  slotId,
  zip,
  onChange,
  result,
  isBest,
}: {
  slotId: "A" | "B" | "C";
  zip: string;
  onChange: (value: string) => void;
  result?: LocationResult;
  isBest: boolean;
}) {
  const resolvedRegion = result?.result?.location.resolved_region;
  const pricingStatus = result?.result?.pricing.pricing_status_label;

  return (
    <div className={`rounded-[1.5rem] border p-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.03)] ${isBest ? "border-emerald-300/30 bg-[linear-gradient(180deg,rgba(74,222,128,0.10),rgba(74,222,128,0.03))]" : "border-white/10 bg-white/[0.04]"}`}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-[11px] uppercase tracking-[0.18em] text-violet-200">Location {slotId}</p>
          <p className="mt-2 text-sm text-muted">ZIP code candidate</p>
        </div>
        {isBest ? <span className="rounded-full border border-emerald-300/25 bg-emerald-300/10 px-3 py-1 text-xs text-emerald-100">Best for selected objective</span> : null}
      </div>

      <label className="mt-4 block space-y-2">
        <span className="text-sm font-medium text-slate-100">ZIP Code</span>
        <input value={zip} onChange={(event) => onChange(event.target.value)} className={fieldClass()} placeholder="e.g. 90012" />
      </label>

      <div className="mt-4 grid gap-3">
        <div className="rounded-[1.2rem] border border-white/10 bg-black/20 p-3">
          <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Resolved Region</p>
          <p className="mt-1.5 text-sm text-text">{resolvedRegion ?? "Awaiting comparison"}</p>
        </div>
        <div className="rounded-[1.2rem] border border-white/10 bg-black/20 p-3">
          <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Pricing Status</p>
          <p className="mt-1.5 text-sm text-text">{pricingStatus ?? "Not run yet"}</p>
        </div>
      </div>
    </div>
  );
}

function ResultCard({ result, isBest }: { result: LocationResult; isBest: boolean }) {
  if (result.error) {
    return (
      <div className="rounded-[1.5rem] border border-danger/25 bg-danger/10 p-5">
        <div className="flex items-center justify-between gap-3">
          <p className="text-sm font-semibold text-red-100">Location {result.slotId} · {result.zip}</p>
          <span className="rounded-full border border-danger/25 bg-danger/10 px-3 py-1 text-xs text-red-100">Failed</span>
        </div>
        <p className="mt-3 text-sm leading-6 text-red-100/90">{result.error}</p>
      </div>
    );
  }

  if (!result.result) {
    return null;
  }

  const run = result.result;

  return (
    <div className={`rounded-[1.6rem] border p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)] ${isBest ? "border-emerald-300/30 bg-[linear-gradient(180deg,rgba(74,222,128,0.12),rgba(74,222,128,0.03))]" : "border-white/10 bg-white/[0.04]"}`}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-[11px] uppercase tracking-[0.18em] text-violet-200">Location {result.slotId}</p>
          <h3 className="mt-2 text-lg font-semibold text-text">ZIP {result.zip}</h3>
          <p className="mt-1 text-sm text-muted">{run.location.resolved_region}</p>
        </div>
        {isBest ? <span className="rounded-full border border-emerald-300/25 bg-emerald-300/10 px-3 py-1 text-xs text-emerald-100">Best for selected objective</span> : null}
      </div>

      <div className="mt-4 flex flex-wrap gap-2.5">
        {run.pricing.badges.map((badge) => (
          <StatusBadge key={`${result.slotId}-${badge.id}`} {...badge} />
        ))}
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2">
        <div className="rounded-[1.2rem] border border-white/10 bg-black/20 p-3">
          <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Optimized Cost</p>
          <p className="mt-1.5 text-sm font-medium text-text">{formatCurrency(Number(run.metrics.optimized_cost ?? 0))}</p>
        </div>
        <div className="rounded-[1.2rem] border border-white/10 bg-black/20 p-3">
          <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Optimized Carbon</p>
          <p className="mt-1.5 text-sm font-medium text-text">{formatKg(Number(run.metrics.optimized_carbon_kg ?? 0))}</p>
        </div>
        <div className="rounded-[1.2rem] border border-white/10 bg-black/20 p-3">
          <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Savings vs Baseline</p>
          <p className="mt-1.5 text-sm font-medium text-text">{formatCurrency(Number(run.metrics.cost_savings ?? 0))}</p>
        </div>
        <div className="rounded-[1.2rem] border border-white/10 bg-black/20 p-3">
          <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Run Window</p>
          <p className="mt-1.5 text-sm font-medium text-text">{formatDateTime(run.schedule.recommended_window.start)} to {formatDateTime(run.schedule.recommended_window.end)}</p>
        </div>
      </div>
    </div>
  );
}

export function MultiLocationTab({ initialValues, onUseBestLocation }: MultiLocationTabProps) {
  const [comparisonValues, setComparisonValues] = useState<FormValues>(initialValues);
  const [zips, setZips] = useState({ A: "90012", B: "77002", C: "60601" });
  const [results, setResults] = useState<LocationResult[]>([]);
  const [validationError, setValidationError] = useState<string | null>(null);

  const comparisonMutation = useMutation({
    mutationFn: async () => {
      const zipEntries = slotDefinitions.map((slot) => ({ slotId: slot.id, zip: zips[slot.id].trim() }));
      const settled = await Promise.allSettled(
        zipEntries.map(async (entry) => {
          const payload = buildOptimizePayload(comparisonValues, entry.zip);
          const result = await optimizeScenario(payload);
          return { slotId: entry.slotId, zip: entry.zip, result } satisfies LocationResult;
        }),
      );

      return settled.map((entry, index) => {
        const slot = zipEntries[index];
        if (entry.status === "fulfilled") {
          return entry.value;
        }
        return {
          slotId: slot.slotId,
          zip: slot.zip,
          error: entry.reason instanceof Error ? entry.reason.message : "Comparison failed for this location.",
        } satisfies LocationResult;
      });
    },
    onSuccess: (data) => {
      setResults(data);
      setValidationError(null);
    },
  });

  const bestLocation = useMemo(() => pickBestLocation(results, comparisonValues.objective), [results, comparisonValues.objective]);

  const comparisonChart = useMemo(() => {
    const successful = results.filter((entry): entry is LocationResult & { result: OptimizeResponse } => Boolean(entry.result));
    if (!successful.length) {
      return null;
    }

    const isCarbonFocused = comparisonValues.objective === "carbon";
    const series = successful.map((entry, index) => ({
      label: `Location ${entry.slotId} · ${entry.zip}`,
      stroke: comparisonStrokes[index % comparisonStrokes.length],
      rows: isCarbonFocused ? chartRowsFromCarbon(entry.result) : chartRowsFromPrice(entry.result),
    })).filter((entry) => entry.rows.length);

    if (!series.length) {
      return null;
    }

    return {
      title: isCarbonFocused ? "Carbon forecast comparison across candidate locations" : "Electricity price comparison across candidate locations",
      subtitle: isCarbonFocused
        ? "Use this view to compare the forecasted carbon intensity profile for each location under the shared comparison scenario."
        : "Use this view to compare the forecasted electricity price profile for each location under the shared comparison scenario.",
      unitLabel: isCarbonFocused ? "gCO2/kWh" : "$/kWh",
      series,
    };
  }, [results, comparisonValues.objective]);

  const handleRunComparison = () => {
    const zipList = slotDefinitions.map((slot) => zips[slot.id]);
    const validationMessage = validateZips(zipList);
    if (validationMessage) {
      setValidationError(validationMessage);
      return;
    }

    setValidationError(null);
    comparisonMutation.mutate();
  };

  const successfulCount = results.filter((result) => result.result).length;

  return (
    <div className="space-y-6">
      <SectionCard title="Shared Scenario" subtitle="" eyebrow="Scenario">
        <div className="grid gap-4 lg:grid-cols-2 xl:grid-cols-3">
          <label className="space-y-2">
            <span className="text-sm font-medium text-slate-100">Compute Hours</span>
            <input value={comparisonValues.compute_hours_required} onChange={(event) => setComparisonValues((current) => ({ ...current, compute_hours_required: Number(event.target.value) }))} type="number" min={1} max={72} className={fieldClass()} />
          </label>
          <label className="space-y-2">
            <span className="text-sm font-medium text-slate-100">Machine Wattage</span>
            <input value={comparisonValues.machine_watts} onChange={(event) => setComparisonValues((current) => ({ ...current, machine_watts: Number(event.target.value) }))} type="number" min={50} max={500000} className={fieldClass()} />
          </label>
          <label className="space-y-2">
            <span className="text-sm font-medium text-slate-100">Optimization Objective</span>
            <div className="relative">
              <select value={comparisonValues.objective} onChange={(event) => setComparisonValues((current) => ({ ...current, objective: event.target.value as Objective }))} className={selectClass()}>
                <option value="cost">Minimize Cost</option>
                <option value="carbon">Minimize Carbon</option>
                <option value="balanced">Balanced</option>
              </select>
              <span className="pointer-events-none absolute inset-y-0 right-4 flex items-center text-slate-300">v</span>
            </div>
          </label>
          <label className="space-y-2 xl:col-span-2">
            <span className="text-sm font-medium text-slate-100">Deadline</span>
            <input value={comparisonValues.deadline} onChange={(event) => setComparisonValues((current) => ({ ...current, deadline: event.target.value }))} type="datetime-local" className={fieldClass()} />
          </label>
          <div className="grid gap-4 sm:grid-cols-2 xl:col-span-1">
            <label className="space-y-2">
              <span className="text-sm font-medium text-slate-100">Scheduling Strategy</span>
              <div className="relative">
                <select value={comparisonValues.schedule_mode} onChange={(event) => setComparisonValues((current) => ({ ...current, schedule_mode: event.target.value as FormValues["schedule_mode"] }))} className={selectClass()}>
                  <option value="flexible">Flexible</option>
                  <option value="block">Continuous Block</option>
                </select>
                <span className="pointer-events-none absolute inset-y-0 right-4 flex items-center text-slate-300">v</span>
              </div>
            </label>
            <label className="space-y-2">
              <span className="text-sm font-medium text-slate-100">Carbon Estimate Type</span>
              <div className="relative">
                <select value={comparisonValues.carbon_estimation_mode} onChange={(event) => setComparisonValues((current) => ({ ...current, carbon_estimation_mode: event.target.value as FormValues["carbon_estimation_mode"] }))} className={selectClass()}>
                  <option value="forecast_only">Short-Term</option>
                  <option value="forecast_plus_historical_expectation">Extended</option>
                </select>
                <span className="pointer-events-none absolute inset-y-0 right-4 flex items-center text-slate-300">v</span>
              </div>
            </label>
          </div>
        </div>
      </SectionCard>

      <SectionCard title="Location Inputs" subtitle="" eyebrow="Locations">
        <div className="grid gap-4 xl:grid-cols-3">
          {slotDefinitions.map((slot) => (
            <LocationCard
              key={slot.id}
              slotId={slot.id}
              zip={zips[slot.id]}
              onChange={(value) => setZips((current) => ({ ...current, [slot.id]: value }))}
              result={results.find((entry) => entry.slotId === slot.id)}
              isBest={bestLocation?.slotId === slot.id}
            />
          ))}
        </div>
      </SectionCard>

      <SectionCard title="Run Location Comparison" subtitle="" eyebrow="Action">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="text-sm text-slate-100/90">Objective currently driving ranking: {formatObjectiveLabel(comparisonValues.objective)}</p>
            
          </div>
          <div className="flex flex-wrap gap-3">
            <button type="button" onClick={() => { setResults([]); setValidationError(null); }} className="rounded-[1.2rem] border border-white/10 bg-white/[0.04] px-4 py-2.5 text-sm text-slate-100/90 transition hover:bg-white/[0.07]">Reset Results</button>
            <button type="button" onClick={handleRunComparison} disabled={comparisonMutation.isPending} className="rounded-[1.25rem] bg-gradient-to-r from-violet-200 via-fuchsia-300 to-cyan-200 px-5 py-3 text-sm font-semibold text-slate-950 shadow-[0_16px_36px_rgba(139,92,246,0.32)] transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-60">{comparisonMutation.isPending ? "Running Comparison..." : "Run location comparison"}</button>
          </div>
        </div>
        {validationError ? <div className="mt-4 rounded-2xl border border-amber-300/25 bg-amber-300/10 p-4 text-sm text-amber-100">{validationError}</div> : null}
        {comparisonMutation.error instanceof Error ? <div className="mt-4 rounded-2xl border border-danger/25 bg-danger/10 p-4 text-sm text-red-100">{comparisonMutation.error.message}</div> : null}
      </SectionCard>

      {results.length ? (
        <>
          <SectionCard title="Comparison Results" subtitle="" eyebrow="Results">
            <div className="grid gap-4 xl:grid-cols-3">
              {results.map((result) => (
                <ResultCard key={result.slotId} result={result} isBest={bestLocation?.slotId === result.slotId} />
              ))}
            </div>
          </SectionCard>

          <SectionCard title="Decision Summary" subtitle="" eyebrow="Winner">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
              <div>
                <p className="text-lg font-semibold text-text">{bestLocation?.result ? `For the selected objective, ZIP ${bestLocation.zip} is currently the best available location.` : "No successful comparison result is available yet."}</p>
                <p className="mt-2 text-sm text-muted">{successfulCount} of 3 locations returned usable comparison data.</p>
              </div>
              <button type="button" disabled={!bestLocation?.result} onClick={() => bestLocation?.result && onUseBestLocation(bestLocation.zip)} className="rounded-[1.25rem] border border-emerald-300/20 bg-emerald-300/10 px-5 py-3 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/14 disabled:cursor-not-allowed disabled:opacity-50">Use best location in optimizer</button>
            </div>
          </SectionCard>

          {comparisonChart ? (
            <SectionCard
              title="Forecast Comparison"
              subtitle=""
              eyebrow="Chart"
            >
              <MultiSeriesLineChart
                title={comparisonChart.title}
                subtitle={comparisonChart.subtitle}
                unitLabel={comparisonChart.unitLabel}
                series={comparisonChart.series}
              />
            </SectionCard>
          ) : null}
        </>
      ) : null}
    </div>
  );
}







