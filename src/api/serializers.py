from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def _clean_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()
    if pd.isna(value):
        return None
    if isinstance(value, (int, float, bool, str)):
        return value
    return str(value)


def _first_value(df: pd.DataFrame, column: str, default: Any = None) -> Any:
    if column not in df.columns:
        return default
    values = df[column].dropna()
    if values.empty:
        return default
    return _clean_scalar(values.iloc[0])


def infer_interval_minutes(df: pd.DataFrame) -> float:
    if len(df) < 2:
        return 0.0
    timestamps = pd.to_datetime(df["timestamp"], errors="coerce").dropna().sort_values()
    if len(timestamps) < 2:
        return 0.0
    return float(timestamps.diff().dropna().dt.total_seconds().median() / 60.0)


def _df_records(df: pd.DataFrame, columns: list[str]) -> list[dict[str, Any]]:
    working = df.copy()
    for column in columns:
        if column in working.columns:
            if str(column).lower().endswith("timestamp") or column == "timestamp":
                working[column] = pd.to_datetime(working[column], errors="coerce")
    records: list[dict[str, Any]] = []
    for row in working[columns].to_dict(orient="records"):
        records.append({key: _clean_scalar(value) for key, value in row.items()})
    return records


def _format_objective_label(value: str) -> str:
    return {
        "carbon": "Minimize Carbon",
        "cost": "Minimize Cost",
        "balanced": "Balanced",
    }.get(str(value), str(value).replace("_", " ").title())


def _format_pricing_status_label(value: str) -> str:
    return {
        "live_market": "Live Market Active",
        "placeholder": "Fallback Pricing",
    }.get(str(value), str(value).replace("_", " ").title())


def _format_market_label(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {"DAM", "DAY_AHEAD"}:
        return "Day-Ahead"
    return str(value or "").replace("_", " ").title()


def _build_card(card_id: str, title: str, value: Any, supporting_text: str = "", tone: str = "default") -> dict[str, Any]:
    return {
        "id": card_id,
        "title": title,
        "value": _clean_scalar(value),
        "supporting_text": supporting_text,
        "tone": tone,
    }


def build_status_badges(result: dict[str, Any]) -> list[dict[str, Any]]:
    forecast_df = result["forecast"].copy()
    location_info = result.get("location_info", {}) or {}
    pricing_status = _first_value(forecast_df, "pricing_status", "")
    pricing_source = _first_value(forecast_df, "pricing_source", "")
    pricing_market = _format_market_label(_first_value(forecast_df, "pricing_market", ""))
    pricing_node = _first_value(forecast_df, "pricing_node", "")
    badges = [
        {
            "id": "location_lookup",
            "label": "Location Lookup",
            "value": "Live WattTime" if location_info.get("location_lookup_status") == "success" else str(location_info.get("location_lookup_status") or "Unknown"),
            "tone": "positive" if location_info.get("location_lookup_status") == "success" else "warning",
        },
        {
            "id": "pricing_status",
            "label": "Pricing",
            "value": _format_pricing_status_label(pricing_status),
            "tone": "positive" if pricing_status == "live_market" else "warning",
        },
    ]
    if pricing_source:
        badges.append({"id": "pricing_source", "label": "Provider", "value": pricing_source, "tone": "neutral"})
    if pricing_market:
        badges.append({"id": "pricing_market", "label": "Market", "value": pricing_market, "tone": "neutral"})
    if pricing_node:
        badges.append({"id": "pricing_node", "label": "Node/Zone", "value": pricing_node, "tone": "neutral"})
    return badges


def build_input_summary(request_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "zip_code": request_data["zip_code"],
        "compute_hours_required": request_data["compute_hours_required"],
        "deadline": _clean_scalar(pd.to_datetime(request_data["deadline"], errors="coerce")),
        "objective": request_data["objective"],
        "machine_watts": request_data["machine_watts"],
        "carbon_weight": request_data.get("carbon_weight"),
        "price_weight": request_data.get("price_weight"),
        "forecast_mode": request_data.get("forecast_mode"),
        "schedule_mode": request_data.get("schedule_mode"),
        "carbon_estimation_mode": request_data.get("carbon_estimation_mode"),
    }


def build_location_summary(result: dict[str, Any]) -> dict[str, Any]:
    location_info = result.get("location_info", {}) or {}
    return {
        "zip_code": location_info.get("zip_code") or getattr(result.get("workload_input"), "zip_code", ""),
        "latitude": location_info.get("latitude"),
        "longitude": location_info.get("longitude"),
        "resolved_region": result.get("region"),
        "watttime_region_full_name": location_info.get("watttime_region_full_name"),
        "signal_type_used": location_info.get("signal_type_used"),
        "location_lookup_status": location_info.get("location_lookup_status"),
    }


def build_pricing_summary(result: dict[str, Any]) -> dict[str, Any]:
    forecast_df = result["forecast"].copy()
    pricing_status = _first_value(forecast_df, "pricing_status", "")
    pricing_market = _first_value(forecast_df, "pricing_market", "")
    return {
        "pricing_status": pricing_status,
        "pricing_status_label": _format_pricing_status_label(pricing_status),
        "pricing_source": _first_value(forecast_df, "pricing_source", ""),
        "pricing_market": pricing_market,
        "pricing_market_label": _format_market_label(pricing_market),
        "pricing_node": _first_value(forecast_df, "pricing_node", ""),
        "pricing_region_code": _first_value(forecast_df, "pricing_region_code", result.get("region")),
        "price_signal_source": _first_value(forecast_df, "price_signal_source", ""),
        "pricing_message": _first_value(forecast_df, "pricing_message", ""),
        "live_price_rows": int((forecast_df.get("price_signal_source") == "live_forecast").sum()) if "price_signal_source" in forecast_df.columns else 0,
        "fallback_price_rows": int((forecast_df.get("price_signal_source") == "placeholder").sum()) if "price_signal_source" in forecast_df.columns else 0,
        "badges": build_status_badges(result),
    }


def build_forecast_summary(result: dict[str, Any]) -> dict[str, Any]:
    forecast_df = result["forecast"].copy()
    timestamps = pd.to_datetime(forecast_df["timestamp"], errors="coerce")
    return {
        "row_count": len(forecast_df),
        "interval_minutes": infer_interval_minutes(forecast_df),
        "window_start": _clean_scalar(timestamps.min()),
        "window_end": _clean_scalar(timestamps.max()),
        "carbon_signal_mix": sorted([str(value) for value in forecast_df.get("carbon_source", pd.Series(dtype=object)).dropna().unique().tolist()]),
        "price_signal_mix": sorted([str(value) for value in forecast_df.get("price_signal_source", pd.Series(dtype=object)).dropna().unique().tolist()]),
    }


def build_summary(result: dict[str, Any]) -> dict[str, Any]:
    optimized_df = result["optimized"].copy()
    selected_df = optimized_df[optimized_df.get("run_flag", 0) == 1].copy()
    timestamps = pd.to_datetime(selected_df.get("timestamp"), errors="coerce") if not selected_df.empty else pd.Series(dtype="datetime64[ns]")
    schedule_mode = "unknown"
    if not selected_df.empty:
        diffs = pd.to_datetime(selected_df["timestamp"], errors="coerce").sort_values().diff().dropna()
        interval_minutes = infer_interval_minutes(selected_df if len(selected_df) > 1 else optimized_df)
        expected = pd.Timedelta(minutes=interval_minutes) if interval_minutes > 0 else None
        is_contiguous = bool(expected is not None and (diffs == expected).all()) if not diffs.empty else True
        schedule_mode = "block" if is_contiguous and len(selected_df) > 1 else "flexible"
    metrics = result["metrics"]
    region = str(result.get("region") or "")
    pricing_status = _first_value(result["forecast"], "pricing_status", "")
    objective = getattr(result.get("workload_input"), "objective", "")
    cards = [
        _build_card("optimized_cost", "Optimized Cost", f"${metrics.get('optimized_cost', 0):.2f}", tone="positive"),
        _build_card("baseline_cost", "Baseline Cost", f"${metrics.get('baseline_cost', 0):.2f}"),
        _build_card("cost_savings", "Cost Savings", f"${metrics.get('cost_savings', 0):.2f}", f"{metrics.get('cost_reduction_pct', 0):.1f}% lower", "positive"),
        _build_card("optimized_carbon", "Optimized Carbon", f"{metrics.get('optimized_carbon_kg', 0):.2f} kg", tone="positive"),
        _build_card("baseline_carbon", "Baseline Carbon", f"{metrics.get('baseline_carbon_kg', 0):.2f} kg"),
        _build_card("carbon_reduction", "Carbon Reduction", f"{metrics.get('carbon_savings_kg', 0):.2f} kg", f"{metrics.get('carbon_reduction_pct', 0):.1f}% lower", "positive"),
        _build_card("objective", "Objective", _format_objective_label(objective)),
        _build_card("resolved_region", "Resolved Region", region),
        _build_card("pricing_status", "Pricing Status", _format_pricing_status_label(pricing_status)),
    ]
    return {
        "recommended_start": _clean_scalar(timestamps.min()) if not timestamps.empty else None,
        "recommended_end": _clean_scalar(timestamps.max()) if not timestamps.empty else None,
        "selected_interval_count": int(selected_df.get("run_flag", pd.Series(dtype=int)).sum()) if not selected_df.empty else 0,
        "eligible_interval_count": int(optimized_df.get("eligible_flag", pd.Series(dtype=int)).sum()) if "eligible_flag" in optimized_df.columns else 0,
        "objective": objective,
        "schedule_mode": schedule_mode,
        "headline": "Optimization completed",
        "subheadline": f"Util evaluated the feasible intervals for {_format_objective_label(objective).lower()} and selected the strongest available run window.",
        "cards": cards,
        "badges": build_status_badges(result),
    }


def build_metrics_summary(result: dict[str, Any]) -> dict[str, Any]:
    metrics = result["metrics"]
    return {key: _clean_scalar(value) for key, value in metrics.items()}


def build_schedule_summary(result: dict[str, Any]) -> dict[str, Any]:
    schedule_df = result["schedule"].copy()
    optimized_df = result["optimized"].copy()
    selected_df = schedule_df[schedule_df.get("run_flag", 0) == 1].copy()
    selected_timestamps = pd.to_datetime(selected_df.get("timestamp"), errors="coerce") if not selected_df.empty else pd.Series(dtype="datetime64[ns]")
    pricing_status = _first_value(result["forecast"], "pricing_status", "")
    return {
        "interval_minutes": infer_interval_minutes(schedule_df),
        "recommended_window": {
            "start": _clean_scalar(selected_timestamps.min()) if not selected_timestamps.empty else None,
            "end": _clean_scalar(selected_timestamps.max()) if not selected_timestamps.empty else None,
            "selected_interval_count": int(selected_df.get("run_flag", pd.Series(dtype=int)).sum()) if not selected_df.empty else 0,
        },
        "status": "ready" if not selected_df.empty else "empty",
        "explanation": (
            "Selected intervals are using live market pricing."
            if pricing_status == "live_market"
            else "Selected intervals were generated with fallback pricing."
        ),
        "selected_intervals": _df_records(
            selected_df,
            [
                "timestamp",
                "recommended_action",
                "price_per_kwh",
                "carbon_g_per_kwh",
            ],
        ),
        "table_rows": _df_records(
            schedule_df.head(200),
            [
                "timestamp",
                "recommended_action",
                "price_per_kwh",
                "carbon_g_per_kwh",
            ],
        ),
        "optimizer_table": _df_records(
            optimized_df.head(200),
            [
                "timestamp",
                "eligible_flag",
                "run_flag",
                "score",
                "price_per_kwh",
                "carbon_g_per_kwh",
            ],
        ),
    }


def build_chart_payloads(result: dict[str, Any]) -> dict[str, Any]:
    optimized_df = result["optimized"].copy()
    forecast_df = result["forecast"].copy()
    return {
        "price_timeseries": {
            "title": "Electricity Price",
            "subtitle": "Price signal aligned to the optimizer interval grid.",
            "x_label": "Timestamp",
            "y_label": "Price ($/kWh)",
            "rows": _df_records(optimized_df, ["timestamp", "price_per_kwh"]),
        },
        "carbon_timeseries": {
            "title": "Carbon Intensity",
            "subtitle": "Carbon signal aligned to the optimizer interval grid.",
            "x_label": "Timestamp",
            "y_label": "Carbon (gCO2/kWh)",
            "rows": _df_records(optimized_df, ["timestamp", "carbon_g_per_kwh"]),
        },
        "run_schedule_timeseries": {
            "title": "Recommended Run Schedule",
            "subtitle": "Run flags over the forecast horizon.",
            "x_label": "Timestamp",
            "y_label": "Run Flag",
            "rows": _df_records(optimized_df, ["timestamp", "run_flag", "eligible_flag", "price_per_kwh", "carbon_g_per_kwh"]),
        },
        "baseline_vs_optimized_comparison": {
            "title": "Baseline vs Optimized",
            "subtitle": "Headline performance deltas for the selected run.",
            "x_label": "Metric",
            "y_label": "Value",
            "rows": [
                {"metric": "Cost", "baseline": _clean_scalar(result["metrics"].get("baseline_cost")), "optimized": _clean_scalar(result["metrics"].get("optimized_cost")), "unit": "USD"},
                {"metric": "Carbon", "baseline": _clean_scalar(result["metrics"].get("baseline_carbon_kg")), "optimized": _clean_scalar(result["metrics"].get("optimized_carbon_kg")), "unit": "kg CO2"},
            ],
        },
        "raw_timeseries": _df_records(
            optimized_df,
            [column for column in ["timestamp", "price_per_kwh", "carbon_g_per_kwh", "run_flag", "eligible_flag"] if column in optimized_df.columns],
        ),
    }


def build_provenance_summary(result: dict[str, Any]) -> dict[str, Any]:
    forecast_df = result["forecast"].copy()
    workload = result["workload_input"]
    location_info = result.get("location_info", {}) or {}
    return {
        "zip_code": getattr(workload, "zip_code", ""),
        "resolved_region": result.get("region"),
        "location_lookup_status": location_info.get("location_lookup_status"),
        "carbon_source": sorted([str(value) for value in forecast_df.get("carbon_source", pd.Series(dtype=object)).dropna().unique().tolist()]),
        "pricing_status": _first_value(forecast_df, "pricing_status", ""),
        "pricing_source": _first_value(forecast_df, "pricing_source", ""),
        "pricing_market": _first_value(forecast_df, "pricing_market", ""),
        "pricing_node": _first_value(forecast_df, "pricing_node", ""),
        "price_signal_source": sorted([str(value) for value in forecast_df.get("price_signal_source", pd.Series(dtype=object)).dropna().unique().tolist()]),
        "objective": getattr(workload, "objective", ""),
        "coverage_note": _first_value(forecast_df, "pricing_message", ""),
    }


def build_diagnostics_summary(result: dict[str, Any]) -> dict[str, Any]:
    forecast_df = result["forecast"].copy()
    return {
        "forecast_rows": len(forecast_df),
        "optimizer_rows": len(result["optimized"]),
        "schedule_rows": len(result["schedule"]),
        "non_null_price_rows": int(pd.to_numeric(forecast_df.get("price_per_kwh"), errors="coerce").notna().sum()) if "price_per_kwh" in forecast_df.columns else 0,
        "price_interval_minutes": infer_interval_minutes(forecast_df),
        "price_alignment_method": "merge_asof_backward",
        "price_resampling_behavior": "Hourly live prices are repeated forward across 5-minute optimizer intervals until the next hourly value arrives.",
    }


def build_export_artifact_list(export_package: dict[str, Any]) -> list[dict[str, Any]]:
    files = export_package.get("files", [])
    artifacts: list[dict[str, Any]] = []
    for path_value in files:
        path = Path(path_value)
        artifacts.append(
            {
                "filename": path.name,
                "display_name": path.name.replace("util_", "").replace(".csv", "").replace("_", " ").title(),
                "artifact_type": "csv",
                "path": str(path),
                "reference_path": str(path),
                "size_bytes": path.stat().st_size if path.exists() else 0,
            }
        )
    return artifacts
