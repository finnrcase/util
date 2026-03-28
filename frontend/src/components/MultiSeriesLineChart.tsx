import { useMemo, useState } from "react";
import type { ChartPoint } from "../features/dashboard/utils";

interface SeriesConfig {
  label: string;
  stroke: string;
  rows: ChartPoint[];
}

interface MultiSeriesLineChartProps {
  title: string;
  subtitle: string;
  unitLabel: string;
  series: SeriesConfig[];
}

function formatTickLabel(timestamp: string): string {
  const date = new Date(timestamp);
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function formatAxisTimeLabel(timestamp: string): string {
  const date = new Date(timestamp);
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
  }).format(date);
}

function formatValue(value: number, unitLabel: string): string {
  if (unitLabel === "$/kWh") {
    return `$${value.toFixed(4)}/kWh`;
  }
  if (unitLabel === "gCO2/kWh") {
    return `${value.toFixed(1)} gCO2/kWh`;
  }
  return `${value.toFixed(3)} ${unitLabel}`;
}

function buildTimeTicks(timestamps: string[], targetMarkers = 6): string[] {
  if (!timestamps.length) {
    return [];
  }

  if (timestamps.length <= targetMarkers) {
    return timestamps;
  }

  const ticks: string[] = [];
  const maxIndex = timestamps.length - 1;

  for (let markerIndex = 0; markerIndex < targetMarkers; markerIndex += 1) {
    const ratio = targetMarkers === 1 ? 0 : markerIndex / (targetMarkers - 1);
    const tickIndex = Math.round(ratio * maxIndex);
    const timestamp = timestamps[tickIndex];
    if (ticks[ticks.length - 1] !== timestamp) {
      ticks.push(timestamp);
    }
  }

  const firstTimestamp = timestamps[0];
  const lastTimestamp = timestamps[maxIndex];
  if (ticks[0] !== firstTimestamp) {
    ticks.unshift(firstTimestamp);
  }
  if (ticks[ticks.length - 1] !== lastTimestamp) {
    ticks.push(lastTimestamp);
  }

  return ticks;
}

export function MultiSeriesLineChart({ title, subtitle, unitLabel, series }: MultiSeriesLineChartProps) {
  const [activeTimestamp, setActiveTimestamp] = useState<string | null>(null);

  const populatedSeries = series.filter((entry) => entry.rows.length);
  if (!populatedSeries.length) {
    return <div className="rounded-[1.6rem] border border-dashed border-border/80 bg-ink/30 p-8 text-sm text-muted">Run a multi-location comparison to visualize the shared forecast horizon.</div>;
  }

  const timestampIndex = new Map<string, number>();
  for (const entry of populatedSeries) {
    for (const row of entry.rows) {
      if (!timestampIndex.has(row.timestamp)) {
        timestampIndex.set(row.timestamp, new Date(row.timestamp).getTime());
      }
    }
  }

  const orderedTimestamps = [...timestampIndex.entries()]
    .sort((left, right) => left[1] - right[1])
    .map(([timestamp]) => timestamp);

  const values = populatedSeries.flatMap((entry) => entry.rows.map((row) => row.value)).filter((value) => Number.isFinite(value));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const valueRange = max - min || 1;

  const width = 1080;
  const height = 360;
  const paddingLeft = 64;
  const paddingRight = 28;
  const paddingTop = 24;
  const paddingBottom = 58;
  const xStep = orderedTimestamps.length > 1 ? (width - paddingLeft - paddingRight) / (orderedTimestamps.length - 1) : 0;

  const chartSeries = populatedSeries.map((entry) => {
    const pointMap = new Map(entry.rows.map((row) => [row.timestamp, row.value]));
    const coords = orderedTimestamps.map((timestamp, index) => {
      const value = pointMap.get(timestamp);
      if (value === undefined) {
        return null;
      }

      const x = paddingLeft + index * xStep;
      const y = height - paddingBottom - ((value - min) / valueRange) * (height - paddingTop - paddingBottom);
      return { timestamp, value, x, y };
    });

    const polylinePoints = coords
      .filter((point): point is NonNullable<typeof point> => Boolean(point))
      .map((point) => `${point.x},${point.y}`)
      .join(" ");

    return {
      ...entry,
      coords,
      polylinePoints,
    };
  });

  const activeTimestampValue = activeTimestamp ?? orderedTimestamps[0];
  const activeIndex = Math.max(0, orderedTimestamps.indexOf(activeTimestampValue));
  const activeX = paddingLeft + activeIndex * xStep;
  const activeRows = chartSeries
    .map((entry) => {
      const point = entry.coords[activeIndex];
      if (!point) {
        return null;
      }
      return {
        label: entry.label,
        stroke: entry.stroke,
        value: point.value,
      };
    })
    .filter((row): row is NonNullable<typeof row> => Boolean(row));

  const yTicks = [max, min + valueRange * 0.5, min];
  const timeTicks = buildTimeTicks(orderedTimestamps, 6);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="text-base font-semibold text-text">{title}</h3>
          <p className="mt-1 text-sm text-muted">{subtitle}</p>
        </div>
        <div className="flex flex-wrap gap-2">
          {chartSeries.map((entry) => (
            <div key={entry.label} className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-3 py-1.5 text-xs text-slate-200">
              <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: entry.stroke }} />
              {entry.label}
            </div>
          ))}
        </div>
      </div>

      <div className="relative overflow-hidden rounded-[1.7rem] border border-white/10 bg-[linear-gradient(180deg,rgba(8,10,20,0.95),rgba(12,14,26,0.92))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.03),0_30px_58px_rgba(34,10,72,0.28)]">
        {activeRows.length ? (
          <div className="pointer-events-none absolute left-5 top-5 z-10 rounded-2xl border border-white/10 bg-[#0a1020]/94 px-4 py-3 shadow-[0_18px_40px_rgba(2,6,23,0.48)] backdrop-blur-xl">
            <p className="text-[11px] uppercase tracking-[0.16em] text-muted">{formatTickLabel(activeTimestampValue)}</p>
            <div className="mt-2 space-y-1.5">
              {activeRows.map((row) => (
                <div key={row.label} className="flex items-center gap-2 text-sm text-text">
                  <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: row.stroke }} />
                  <span className="text-slate-200">{row.label}</span>
                  <span className="text-muted">{formatValue(row.value, unitLabel)}</span>
                </div>
              ))}
            </div>
          </div>
        ) : null}

        <svg viewBox={`0 0 ${width} ${height}`} className="w-full overflow-visible">
          {[0, 0.5, 1].map((ratio, index) => {
            const y = paddingTop + (height - paddingTop - paddingBottom) * ratio;
            return <line key={index} x1={paddingLeft} y1={y} x2={width - paddingRight} y2={y} stroke="rgba(157, 167, 190, 0.12)" strokeDasharray="5 7" />;
          })}

          <line x1={paddingLeft} y1={height - paddingBottom} x2={width - paddingRight} y2={height - paddingBottom} stroke="rgba(157, 167, 190, 0.18)" />
          <line x1={paddingLeft} y1={paddingTop} x2={paddingLeft} y2={height - paddingBottom} stroke="rgba(157, 167, 190, 0.12)" />

          {yTicks.map((tick, index) => {
            const y = paddingTop + ((max - tick) / valueRange) * (height - paddingTop - paddingBottom);
            return (
              <text key={`${tick}-${index}`} x={paddingLeft - 12} y={y + 4} textAnchor="end" fill="rgba(226,232,240,0.74)" fontSize="12">
                {formatValue(tick, unitLabel).replace("/kWh", "")}
              </text>
            );
          })}

          {timeTicks.map((timestamp) => {
            const tickIndex = orderedTimestamps.indexOf(timestamp);
            const x = paddingLeft + tickIndex * xStep;
            return (
              <g key={`tick-${timestamp}`}>
                <line x1={x} y1={height - paddingBottom} x2={x} y2={height - paddingBottom + 6} stroke="rgba(157,167,190,0.3)" />
                <text x={x} y={height - 16} textAnchor="middle" fill="rgba(226,232,240,0.74)" fontSize="12">
                  {formatAxisTimeLabel(timestamp)}
                </text>
              </g>
            );
          })}

          {activeRows.length ? <line x1={activeX} y1={paddingTop} x2={activeX} y2={height - paddingBottom} stroke="rgba(226,232,240,0.18)" strokeDasharray="4 6" /> : null}

          {chartSeries.map((entry) => (
            <polyline
              key={entry.label}
              fill="none"
              stroke={entry.stroke}
              strokeWidth="3"
              points={entry.polylinePoints}
              strokeLinejoin="round"
              strokeLinecap="round"
            />
          ))}

          {chartSeries.flatMap((entry) =>
            entry.coords.map((point, index) => {
              if (!point) {
                return null;
              }
              return (
                <circle
                  key={`${entry.label}-${point.timestamp}-${index}`}
                  cx={point.x}
                  cy={point.y}
                  r="9"
                  fill="transparent"
                  onMouseEnter={() => setActiveTimestamp(point.timestamp)}
                  onFocus={() => setActiveTimestamp(point.timestamp)}
                />
              );
            }),
          )}
        </svg>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3 text-xs text-muted">
        <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.03] px-3 py-1.5">
          <span>{unitLabel}</span>
        </div>
        <div className="text-right">
          <span>{formatTickLabel(orderedTimestamps[0])}</span>
          <span className="mx-2 text-white/20">to</span>
          <span>{formatTickLabel(orderedTimestamps[orderedTimestamps.length - 1])}</span>
        </div>
      </div>
    </div>
  );
}
