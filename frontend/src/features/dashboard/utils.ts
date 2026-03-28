import type { OptimizeResponse, TimeseriesRow } from "../../types/api";

export interface ChartPoint {
  timestamp: string;
  value: number;
}

export interface ScheduleDisplayRow {
  timestamp: string;
  recommendedAction: string;
  runPercent: number;
  pricePerKwh?: number;
  carbonPerKwh?: number;
  recommended: boolean;
  eligible: boolean;
}

export interface ForecastStatItem {
  label: string;
  value: string;
}

export function formatDateTime(value?: string | null): string {
  if (!value) {
    return "--";
  }

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(value));
}

export function formatShortDate(value?: string | null): string {
  if (!value) {
    return "--";
  }

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
  }).format(new Date(value));
}

export function formatNumber(value?: number | null, digits = 4): string {
  if (value === undefined || value === null || Number.isNaN(value)) {
    return "--";
  }

  return value.toFixed(digits);
}

export function formatPercent(value?: number | null, digits = 0): string {
  if (value === undefined || value === null || Number.isNaN(value)) {
    return "--";
  }

  return `${value.toFixed(digits)}%`;
}

export function formatAction(action?: string): string {
  if (!action) {
    return "Wait";
  }

  return action.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

export function formatObjective(value: string): string {
  return value.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

export function formatCurrencyPerKwh(value?: number | null): string {
  if (value === undefined || value === null || Number.isNaN(value)) {
    return "--";
  }

  return `$${value.toFixed(4)}/kWh`;
}

export function formatCarbonPerKwh(value?: number | null): string {
  if (value === undefined || value === null || Number.isNaN(value)) {
    return "--";
  }

  return `${value.toFixed(1)} gCO2/kWh`;
}

export function formatKg(value?: number | null): string {
  if (value === undefined || value === null || Number.isNaN(value)) {
    return "--";
  }

  return `${value.toFixed(2)} kg CO2`;
}

export function chartRowsFromPrice(data?: OptimizeResponse): ChartPoint[] {
  return (data?.charts.price_timeseries.rows ?? []).map((row) => ({
    timestamp: row.timestamp,
    value: row.price_per_kwh,
  }));
}

export function chartRowsFromCarbon(data?: OptimizeResponse): ChartPoint[] {
  return (data?.charts.carbon_timeseries.rows ?? []).map((row) => ({
    timestamp: row.timestamp,
    value: row.carbon_g_per_kwh,
  }));
}

export function recommendedTimestamps(data?: OptimizeResponse): string[] {
  const rows = data?.charts.run_schedule_timeseries.rows ?? [];
  return rows.filter((row) => row.run_flag === 1).map((row) => row.timestamp);
}

export function deriveScheduleRows(data?: OptimizeResponse): ScheduleDisplayRow[] {
  if (!data) {
    return [];
  }

  const scheduleRows = data.schedule.table_rows ?? [];
  const runIndex = new Map<string, TimeseriesRow>();
  for (const row of data.charts.run_schedule_timeseries.rows ?? []) {
    runIndex.set(row.timestamp, row);
  }

  return scheduleRows.map((row) => {
    const matched = runIndex.get(row.timestamp);
    const recommended = matched?.run_flag === 1;
    return {
      timestamp: row.timestamp,
      recommendedAction: formatAction(row.recommended_action),
      runPercent: recommended ? 100 : 0,
      pricePerKwh: row.price_per_kwh ?? matched?.price_per_kwh,
      carbonPerKwh: row.carbon_g_per_kwh ?? matched?.carbon_g_per_kwh,
      recommended,
      eligible: matched?.eligible_flag === 1,
    };
  });
}

export function recommendationText(data?: OptimizeResponse): string {
  if (!data) {
    return "Run an optimization to generate a recommended execution window.";
  }

  const start = formatDateTime(data.schedule.recommended_window.start);
  const end = formatDateTime(data.schedule.recommended_window.end);
  const source = data.pricing.pricing_status_label || "available pricing";
  const region = data.location.resolved_region || "the resolved region";
  return `Recommended to run primarily between ${start} and ${end} based on lower forecasted price and carbon intervals for ${region}, using ${source.toLowerCase()}.`;
}

export function completionStatus(data?: OptimizeResponse): string {
  if (!data) {
    return "Awaiting run";
  }

  const deadline = new Date(data.input.deadline).getTime();
  const end = data.schedule.recommended_window.end ? new Date(data.schedule.recommended_window.end).getTime() : null;
  if (!end) {
    return "No run window";
  }

  return end <= deadline ? "Completes before deadline" : "Close to deadline";
}

export function compactRuntimeLabel(data?: OptimizeResponse): string {
  if (!data) {
    return "--";
  }

  const hours = data.input.compute_hours_required;
  return `${hours} ${hours === 1 ? "hour" : "hours"}`;
}

export function computeRangeStats(rows: ChartPoint[]) {
  if (!rows.length) {
    return null;
  }

  const values = rows.map((row) => row.value).filter((value) => Number.isFinite(value));
  if (!values.length) {
    return null;
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const avg = values.reduce((sum, value) => sum + value, 0) / values.length;
  return { min, max, avg };
}

export function countRecommendedIntervals(rows: ChartPoint[], markerTimestamps: string[]): number {
  const markerSet = new Set(markerTimestamps);
  return rows.reduce((count, row) => count + (markerSet.has(row.timestamp) ? 1 : 0), 0);
}

export function buildPriceForecastStats(data: OptimizeResponse, rows: ChartPoint[], markerTimestamps: string[]): ForecastStatItem[] {
  const stats = computeRangeStats(rows);
  const recommendedCount = countRecommendedIntervals(rows, markerTimestamps);

  return [
    { label: "Min", value: formatCurrencyPerKwh(stats?.min) },
    { label: "Avg", value: formatCurrencyPerKwh(stats?.avg) },
    { label: "Max", value: formatCurrencyPerKwh(stats?.max) },
    { label: "Recommended Intervals", value: String(recommendedCount) },
    { label: "Forecast Start", value: formatDateTime(data.forecast.window_start) },
    { label: "Forecast End", value: formatDateTime(data.forecast.window_end) },
  ];
}

export function buildCarbonForecastStats(data: OptimizeResponse, rows: ChartPoint[], markerTimestamps: string[]): ForecastStatItem[] {
  const stats = computeRangeStats(rows);
  const recommendedCount = countRecommendedIntervals(rows, markerTimestamps);
  const optimizedCarbonKg = typeof data.metrics.optimized_carbon_kg === "number" ? data.metrics.optimized_carbon_kg : null;

  const items: ForecastStatItem[] = [
    { label: "Min", value: formatCarbonPerKwh(stats?.min) },
    { label: "Avg", value: formatCarbonPerKwh(stats?.avg) },
    { label: "Max", value: formatCarbonPerKwh(stats?.max) },
    { label: "Recommended Intervals", value: String(recommendedCount) },
  ];

  if (optimizedCarbonKg !== null) {
    items.push({ label: "Estimated Recommended-Window Carbon", value: formatKg(optimizedCarbonKg) });
  }

  items.push({ label: "Forecast Start", value: formatDateTime(data.forecast.window_start) });
  items.push({ label: "Forecast End", value: formatDateTime(data.forecast.window_end) });
  return items;
}

export function buildPriceInterpretation(rows: ChartPoint[], markerTimestamps: string[]): string {
  const markerSet = new Set(markerTimestamps);
  const recommendedValues = rows.filter((row) => markerSet.has(row.timestamp)).map((row) => row.value);
  const allStats = computeRangeStats(rows);
  if (!allStats || !recommendedValues.length) {
    return "Green points indicate intervals selected by the optimizer.";
  }

  const recommendedAvg = recommendedValues.reduce((sum, value) => sum + value, 0) / recommendedValues.length;
  return recommendedAvg <= allStats.avg
    ? "Recommended intervals cluster during the lower-price portion of the forecast window."
    : "Recommended intervals remain visible on the forecast, even when price is not the only objective driver.";
}

export function buildCarbonInterpretation(rows: ChartPoint[], markerTimestamps: string[]): string {
  const markerSet = new Set(markerTimestamps);
  const recommendedValues = rows.filter((row) => markerSet.has(row.timestamp)).map((row) => row.value);
  const allStats = computeRangeStats(rows);
  if (!allStats || !recommendedValues.length) {
    return "Green points indicate intervals selected by the optimizer.";
  }

  const recommendedAvg = recommendedValues.reduce((sum, value) => sum + value, 0) / recommendedValues.length;
  return recommendedAvg <= allStats.avg
    ? "Util selected lower-carbon intervals within the available forecast horizon."
    : "Selected intervals balance carbon conditions with the broader optimization objective and deadline.";
}
