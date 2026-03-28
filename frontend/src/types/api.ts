export type Objective = "carbon" | "cost" | "balanced";

export interface OptimizeRequest {
  zip_code: string;
  compute_hours_required: number;
  deadline: string;
  objective: Objective;
  machine_watts: number;
  carbon_weight?: number;
  price_weight?: number;
  forecast_mode?: "demo" | "live_carbon";
  schedule_mode?: "flexible" | "block";
  carbon_estimation_mode?: "forecast_only" | "forecast_plus_historical_expectation";
  historical_days?: number;
  current_time_override?: string | null;
  include_diagnostics?: boolean;
}

export interface ExportRequest extends OptimizeRequest {
  export_root?: string | null;
  enable_cloud_upload?: boolean;
}

export interface CoverageMarket {
  market: string;
  coverage: string;
  examples: string[];
  status: string;
}

export interface CoverageNote {
  market: string;
  note: string;
}

export interface CoverageResponse {
  summary: string;
  supported_live_markets: CoverageMarket[];
  partially_supported_notes: CoverageNote[];
  unsupported_behavior: {
    status: string;
    label: string;
    message: string;
  };
  notes: string[];
}

export interface ExportArtifact {
  filename: string;
  display_name: string;
  artifact_type: string;
  path: string;
  reference_path: string;
  size_bytes: number;
}

export interface ExportResponse {
  export_dir: string;
  run_id: string;
  artifacts: ExportArtifact[];
  cloud_upload_enabled: boolean;
  cloud_message: string;
  summary?: {
    artifact_count?: number;
    includes_provenance_summary?: boolean;
    export_type?: string;
  } | null;
}

export interface SummaryCardModel {
  id: string;
  title: string;
  value: string | number | null;
  supporting_text?: string;
  tone?: "default" | "positive" | "warning";
}

export interface StatusBadgeModel {
  id: string;
  label: string;
  value: string;
  tone: "positive" | "warning" | "neutral";
}

export interface TimeseriesRow {
  timestamp: string;
  recommended_action?: string;
  price_per_kwh?: number;
  carbon_g_per_kwh?: number;
  run_flag?: number;
  eligible_flag?: number;
}

export interface ChartPayload<Row = TimeseriesRow> {
  title: string;
  subtitle: string;
  x_label: string;
  y_label: string;
  rows: Row[];
}

export interface OptimizeResponse {
  input: {
    zip_code: string;
    compute_hours_required: number;
    deadline: string;
    objective: Objective;
    machine_watts: number;
    carbon_weight?: number;
    price_weight?: number;
    forecast_mode?: string;
    schedule_mode?: string;
    carbon_estimation_mode?: string;
  };
  location: {
    zip_code?: string;
    latitude?: number;
    longitude?: number;
    resolved_region: string;
    watttime_region_full_name?: string;
    signal_type_used?: string;
    location_lookup_status: string;
  };
  pricing: {
    pricing_status: string;
    pricing_status_label: string;
    pricing_source: string;
    pricing_market: string;
    pricing_market_label: string;
    pricing_node: string;
    pricing_region_code?: string;
    price_signal_source?: string;
    pricing_message?: string;
    live_price_rows?: number;
    fallback_price_rows?: number;
    badges: StatusBadgeModel[];
  };
  forecast: {
    row_count: number;
    interval_minutes: number;
    window_start: string | null;
    window_end: string | null;
    carbon_signal_mix: string[];
    price_signal_mix: string[];
  };
  summary: {
    recommended_start?: string | null;
    recommended_end?: string | null;
    selected_interval_count?: number;
    eligible_interval_count?: number;
    objective?: string;
    schedule_mode?: string;
    headline: string;
    subheadline: string;
    cards: SummaryCardModel[];
    badges: StatusBadgeModel[];
  };
  metrics: Record<string, number | string | null>;
  schedule: {
    recommended_window: {
      start: string | null;
      end: string | null;
      selected_interval_count: number;
    };
    status: string;
    explanation: string;
    selected_intervals: TimeseriesRow[];
    table_rows: TimeseriesRow[];
    optimizer_table?: TimeseriesRow[];
  };
  charts: {
    price_timeseries: ChartPayload<{ timestamp: string; price_per_kwh: number }>;
    carbon_timeseries: ChartPayload<{ timestamp: string; carbon_g_per_kwh: number }>;
    run_schedule_timeseries: ChartPayload<TimeseriesRow>;
    baseline_vs_optimized_comparison: ChartPayload<{ metric: string; baseline: number; optimized: number; unit: string }>;
    raw_timeseries?: TimeseriesRow[];
  };
  provenance: {
    zip_code: string;
    resolved_region: string;
    location_lookup_status: string;
    carbon_source: string[];
    pricing_status: string;
    pricing_source: string;
    pricing_market: string;
    pricing_node: string;
    price_signal_source: string[];
    objective: Objective;
    coverage_note: string;
  };
  diagnostics?: Record<string, unknown> | null;
}
