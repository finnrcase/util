import { ForecastChartCard } from "../../../components/ForecastChartCard";
import { SectionCard } from "../../../components/SectionCard";
import type { OptimizeResponse } from "../../../types/api";
import {
  buildCarbonForecastStats,
  buildCarbonInterpretation,
  buildPriceForecastStats,
  buildPriceInterpretation,
  chartRowsFromCarbon,
  chartRowsFromPrice,
  formatDateTime,
  formatKg,
  recommendedTimestamps,
} from "../utils";

interface ForecastTabProps {
  data?: OptimizeResponse;
}

export function ForecastTab({ data }: ForecastTabProps) {
  if (!data) {
    return <SectionCard title="Forecast Visuals" subtitle=""><div className="rounded-[1.6rem] border border-dashed border-white/10 bg-black/20 p-10 text-center text-muted">No forecast loaded yet. Run the optimizer to populate the forecast views.</div></SectionCard>;
  }

  const markers = recommendedTimestamps(data);
  const comparisonRows = data.charts.baseline_vs_optimized_comparison.rows;
  const priceRows = chartRowsFromPrice(data);
  const carbonRows = chartRowsFromCarbon(data);
  const optimizedCarbonKg = typeof data.metrics.optimized_carbon_kg === "number" ? data.metrics.optimized_carbon_kg : null;

  return (
    <div className="space-y-6">
      <ForecastChartCard
        eyebrow="Electricity Price"
        title="Forecasted electricity price across the optimization horizon"
        subtitle="Green points show selected intervals."
        rows={priceRows}
        stroke="#8b5cf6"
        unitLabel="$/kWh"
        markerTimestamps={markers}
        stats={buildPriceForecastStats(data, priceRows, markers)}
        interpretation={buildPriceInterpretation(priceRows, markers)}
      />

      <ForecastChartCard
        eyebrow="Carbon Intensity"
        title="Forecasted carbon intensity across the optimization horizon"
        subtitle="Green points show selected intervals."
        rows={carbonRows}
        stroke="#38bdf8"
        unitLabel="gCO2/kWh"
        markerTimestamps={markers}
        stats={buildCarbonForecastStats(data, carbonRows, markers)}
        interpretation={buildCarbonInterpretation(carbonRows, markers)}
      />

      <SectionCard title="Forecast Context" subtitle="" eyebrow="Context">
        <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_340px]">
          <div className="rounded-[1.6rem] border border-white/10 bg-white/[0.04] p-5">
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Forecast Window Start</p>
                <p className="mt-2 text-sm text-text">{formatDateTime(data.forecast.window_start)}</p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Forecast Window End</p>
                <p className="mt-2 text-sm text-text">{formatDateTime(data.forecast.window_end)}</p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Recommended Intervals</p>
                <p className="mt-2 text-sm text-text">{markers.length}</p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-black/20 p-4">
                <p className="text-[11px] uppercase tracking-[0.16em] text-muted">Estimated Recommended-Window Carbon</p>
                <p className="mt-2 text-sm text-text">{optimizedCarbonKg !== null ? formatKg(optimizedCarbonKg) : "Not surfaced in current payload"}</p>
              </div>
            </div>
          </div>
          <div className="rounded-[1.6rem] border border-white/10 bg-[linear-gradient(180deg,rgba(74,222,128,0.08),rgba(74,222,128,0.02))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
            <p className="text-[11px] uppercase tracking-[0.16em] text-emerald-200">Baseline vs Optimized</p>
            <div className="mt-4 space-y-4">
              {comparisonRows.map((row) => (
                <div key={row.metric} className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <p className="text-sm font-medium text-text">{row.metric}</p>
                  <p className="mt-2 text-sm text-muted">Baseline {row.baseline} {row.unit}</p>
                  <p className="mt-1 text-sm text-emerald-100">Optimized {row.optimized} {row.unit}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </SectionCard>
    </div>
  );
}




