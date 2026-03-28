import { useMemo, useState } from "react";
import type { ChartPoint } from "../features/dashboard/utils";

interface SimpleLineChartProps {
  rows: ChartPoint[];
  stroke: string;
  unitLabel: string;
  markerTimestamps?: string[];
  markerColor?: string;
  markerLabel?: string;
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

function buildTimeTicks(rows: ChartPoint[], targetMarkers = 6): string[] {
  if (!rows.length) {
    return [];
  }

  if (rows.length <= targetMarkers) {
    return rows.map((row) => row.timestamp);
  }

  const ticks: string[] = [];
  const maxIndex = rows.length - 1;

  for (let markerIndex = 0; markerIndex < targetMarkers; markerIndex += 1) {
    const ratio = targetMarkers === 1 ? 0 : markerIndex / (targetMarkers - 1);
    const rowIndex = Math.round(ratio * maxIndex);
    const timestamp = rows[rowIndex].timestamp;
    if (ticks[ticks.length - 1] !== timestamp) {
      ticks.push(timestamp);
    }
  }

  const firstTimestamp = rows[0].timestamp;
  const lastTimestamp = rows[maxIndex].timestamp;
  if (ticks[0] !== firstTimestamp) {
    ticks.unshift(firstTimestamp);
  }
  if (ticks[ticks.length - 1] !== lastTimestamp) {
    ticks.push(lastTimestamp);
  }

  return ticks;
}

export function SimpleLineChart({
  rows,
  stroke,
  unitLabel,
  markerTimestamps = [],
  markerColor = "#4ade80",
  markerLabel = "Recommended run",
}: SimpleLineChartProps) {
  const [activeTimestamp, setActiveTimestamp] = useState<string | null>(null);

  if (!rows.length) {
    return <div className="rounded-[1.6rem] border border-dashed border-border/80 bg-ink/30 p-8 text-sm text-muted">No chart data available yet.</div>;
  }

  const width = 1080;
  const height = 360;
  const paddingLeft = 64;
  const paddingRight = 24;
  const paddingTop = 20;
  const paddingBottom = 58;
  const values = rows.map((row) => row.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const valueRange = max - min || 1;
  const xStep = rows.length > 1 ? (width - paddingLeft - paddingRight) / (rows.length - 1) : 0;
  const markerSet = useMemo(() => new Set(markerTimestamps), [markerTimestamps]);

  const coords = rows.map((row, index) => {
    const x = paddingLeft + index * xStep;
    const y = height - paddingBottom - ((row.value - min) / valueRange) * (height - paddingTop - paddingBottom);
    return { x, y, ...row, highlighted: markerSet.has(row.timestamp) };
  });

  const coordIndex = new Map(coords.map((row) => [row.timestamp, row]));
  const timeTicks = buildTimeTicks(rows, 6).map((timestamp) => coordIndex.get(timestamp)).filter((row): row is NonNullable<typeof row> => Boolean(row));
  const points = coords.map((row) => `${row.x},${row.y}`).join(" ");
  const areaPoints = `${paddingLeft},${height - paddingBottom} ${points} ${width - paddingRight},${height - paddingBottom}`;
  const gradientId = `fill-${stroke.replace(/[^a-zA-Z0-9]/g, "")}`;
  const highlightedPoints = coords.filter((row) => row.highlighted);
  const activePoint = activeTimestamp ? coords.find((row) => row.timestamp === activeTimestamp) ?? null : null;
  const yTicks = [max, min + valueRange * 0.5, min];

  return (
    <div className="space-y-4">
      <div className="relative overflow-hidden rounded-[1.7rem] border border-white/10 bg-[linear-gradient(180deg,rgba(8,10,20,0.95),rgba(12,14,26,0.92))] p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.03),0_30px_58px_rgba(34,10,72,0.28)]">
        {activePoint ? (
          <div className="pointer-events-none absolute left-5 top-5 z-10 rounded-2xl border border-white/10 bg-[#0a1020]/94 px-4 py-3 shadow-[0_18px_40px_rgba(2,6,23,0.48)] backdrop-blur-xl">
            <p className="text-[11px] uppercase tracking-[0.16em] text-muted">{formatTickLabel(activePoint.timestamp)}</p>
            <p className="mt-2 text-sm font-medium text-text">{formatValue(activePoint.value, unitLabel)}</p>
            <p className="mt-1 text-xs text-emerald-200">{activePoint.highlighted ? markerLabel : "Forecast interval"}</p>
          </div>
        ) : null}

        <svg viewBox={`0 0 ${width} ${height}`} className="w-full overflow-visible">
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={stroke} stopOpacity="0.24" />
              <stop offset="100%" stopColor={stroke} stopOpacity="0" />
            </linearGradient>
          </defs>

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

          {timeTicks.map((tick) => (
            <g key={`tick-${tick.timestamp}`}>
              <line x1={tick.x} y1={height - paddingBottom} x2={tick.x} y2={height - paddingBottom + 6} stroke="rgba(157,167,190,0.3)" />
              <text x={tick.x} y={height - 16} textAnchor="middle" fill="rgba(226,232,240,0.74)" fontSize="12">
                {formatAxisTimeLabel(tick.timestamp)}
              </text>
            </g>
          ))}

          <polygon points={areaPoints} fill={`url(#${gradientId})`} />
          <polyline fill="none" stroke={stroke} strokeWidth="3.5" points={points} strokeLinejoin="round" strokeLinecap="round" />

          {coords.map((point) => (
            <circle
              key={`hit-${point.timestamp}`}
              cx={point.x}
              cy={point.y}
              r="10"
              fill="transparent"
              onMouseEnter={() => setActiveTimestamp(point.timestamp)}
              onFocus={() => setActiveTimestamp(point.timestamp)}
            />
          ))}

          {highlightedPoints.map((point) => (
            <g key={`marker-${point.timestamp}`}>
              <circle cx={point.x} cy={point.y} r="9" fill={markerColor} opacity="0.18" />
              <circle cx={point.x} cy={point.y} r="5.2" fill={markerColor} stroke="rgba(255,255,255,0.85)" strokeWidth="1.2" />
            </g>
          ))}
        </svg>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3 text-xs text-muted">
        <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.03] px-3 py-1.5">
          <span className="h-3 w-3 rounded-full" style={{ backgroundColor: markerColor }} />
          {markerLabel}
        </div>
        <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.03] px-3 py-1.5">
          <span>{unitLabel}</span>
        </div>
        <div className="text-right">
          <span>{formatTickLabel(rows[0].timestamp)}</span>
          <span className="mx-2 text-white/20">to</span>
          <span>{formatTickLabel(rows[rows.length - 1].timestamp)}</span>
        </div>
      </div>
    </div>
  );
}
