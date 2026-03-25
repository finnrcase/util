from __future__ import annotations
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.cloud.s3_storage import upload_run_outputs
from src.runtime_config import get_float_setting
from src.scheduling_window import APP_TIMEZONE


EXPORT_FILENAMES = {
    "recommendation": "util_optimization_recommendation.csv",
    "region_comparison": "util_region_comparison.csv",
    "time_window_analysis": "util_time_window_analysis.csv",
    "case_comparison": "util_case_comparison.csv",
    "input_assumptions": "util_input_assumptions.csv",
    "run_summary": "util_run_summary.csv",
}


def generate_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"util-{timestamp}-{uuid.uuid4().hex[:8]}"


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _format_objective_label(objective: str) -> str:
    return {
        "carbon": "Carbon",
        "cost": "Price",
        "balanced": "Balanced",
    }.get(objective, str(objective).title())


def _coerce_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None or value == "":
        return None

    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None

    if getattr(ts, "tzinfo", None) is None:
        return ts.tz_localize(APP_TIMEZONE)

    return ts.tz_convert(APP_TIMEZONE)


def _format_local_timestamp(value: Any) -> str:
    ts = _coerce_timestamp(value)
    if ts is None:
        return ""
    return ts.strftime("%Y-%m-%d %H:%M:%S %Z")


def _format_utc_timestamp(value: Any) -> str:
    ts = _coerce_timestamp(value)
    if ts is None:
        return ""
    return ts.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S UTC")


def _infer_interval_hours(df: pd.DataFrame) -> float:
    if len(df) < 2:
        return 1.0

    timestamps = pd.to_datetime(df["timestamp"], errors="coerce").dropna().sort_values()
    if len(timestamps) < 2:
        return 1.0

    interval_minutes = timestamps.diff().dropna().dt.total_seconds().median() / 60.0
    if not interval_minutes or interval_minutes <= 0:
        return 1.0

    return interval_minutes / 60.0


def _compute_totals(df: pd.DataFrame, machine_watts: int) -> dict[str, float]:
    if df.empty:
        return {"energy_kwh": 0.0, "cost_usd": 0.0, "emissions_total": 0.0, "avg_emissions_gco2_per_kwh": 0.0}

    interval_hours = _infer_interval_hours(df)
    machine_kw = machine_watts / 1000.0
    energy_per_row_kwh = machine_kw * interval_hours

    price_series = pd.to_numeric(df.get("price_per_kwh"), errors="coerce").fillna(0)
    carbon_series = pd.to_numeric(df.get("carbon_g_per_kwh"), errors="coerce").fillna(0)

    total_energy_kwh = float(energy_per_row_kwh * len(df))
    total_cost = float((price_series * energy_per_row_kwh).sum())
    total_emissions = float((carbon_series * energy_per_row_kwh).sum() / 1000.0)
    avg_emissions = float(carbon_series.mean()) if not carbon_series.empty else 0.0

    return {
        "energy_kwh": total_energy_kwh,
        "cost_usd": total_cost,
        "emissions_total": total_emissions,
        "avg_emissions_gco2_per_kwh": avg_emissions,
    }


def _score_weights(objective: str) -> tuple[float, float]:
    if objective == "carbon":
        return 0.0, 1.0
    if objective == "cost":
        return 1.0, 0.0
    return 0.5, 0.5


def _min_max_normalize(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    min_value = numeric.min()
    max_value = numeric.max()

    if pd.isna(min_value) or pd.isna(max_value):
        return pd.Series(0.0, index=series.index)

    if max_value == min_value:
        return pd.Series(0.0, index=series.index)

    return (numeric - min_value) / (max_value - min_value)


def _build_summary_note(
    *,
    objective: str,
    region: str,
    start_local: str,
    cost_savings_pct: float,
    emissions_reduction_pct: float,
    price_weight: float,
    carbon_weight: float,
) -> str:
    timing_note = f"Start at {start_local}" if start_local else "Adjust the run timing"
    if objective == "balanced":
        return (
            f"{timing_note} and deploy in {region}. This schedule was selected using a weighted "
            f"balance of carbon and electricity cost ({carbon_weight:.0%} carbon / "
            f"{price_weight:.0%} price), reducing cost by {cost_savings_pct:.1f}% and "
            f"emissions by {emissions_reduction_pct:.1f}%."
        )
    return (
        f"{timing_note} and deploy in {region} to reduce cost by "
        f"{cost_savings_pct:.1f}% and emissions by {emissions_reduction_pct:.1f}%."
    )


def _build_candidate_windows(
    schedule_df: pd.DataFrame,
    *,
    machine_watts: int,
    slots_required: int,
    deadline: Any,
    selected_timestamps: set[pd.Timestamp],
    cost_weight: float,
    carbon_weight: float,
) -> pd.DataFrame:
    if schedule_df.empty or slots_required <= 0:
        return pd.DataFrame()

    working_df = schedule_df.copy()
    working_df["timestamp"] = pd.to_datetime(working_df["timestamp"], errors="coerce")
    working_df = working_df.sort_values("timestamp").reset_index(drop=True)

    deadline_ts = _coerce_timestamp(deadline)
    rows: list[dict[str, Any]] = []

    for start_index in range(0, max(len(working_df) - slots_required + 1, 0)):
        window_df = working_df.iloc[start_index:start_index + slots_required].copy()
        if len(window_df) < slots_required:
            continue

        totals = _compute_totals(window_df, machine_watts)
        start_ts = window_df["timestamp"].min()
        end_ts = window_df["timestamp"].max()
        meets_deadline = True if deadline_ts is None else _coerce_timestamp(end_ts) <= deadline_ts
        selected_window = set(window_df["timestamp"].tolist()) == selected_timestamps if selected_timestamps else False

        rows.append(
            {
                "window_start": start_ts,
                "window_end": end_ts,
                "window_duration_hours": round(_infer_interval_hours(window_df) * len(window_df), 3),
                "forecasted_grid_emissions_gco2_per_kwh": totals["avg_emissions_gco2_per_kwh"],
                "forecasted_electricity_price_usd_per_kwh": float(
                    pd.to_numeric(window_df.get("price_per_kwh"), errors="coerce").fillna(0).mean()
                ),
                "projected_electricity_cost_usd": totals["cost_usd"],
                "projected_emissions_total": totals["emissions_total"],
                "meets_deadline": meets_deadline,
                "meets_latency_requirement": True,
                "meets_policy_constraints": True,
                "selected_window": selected_window,
            }
        )

    windows_df = pd.DataFrame(rows)
    if windows_df.empty:
        return windows_df

    windows_df["balanced_score"] = (
        _min_max_normalize(windows_df["projected_electricity_cost_usd"]) * cost_weight
        + _min_max_normalize(windows_df["projected_emissions_total"]) * carbon_weight
    )
    windows_df["window_rank"] = windows_df["balanced_score"].rank(method="dense", ascending=True).astype(int)
    return windows_df


def build_export_frames(
    result: dict[str, Any],
    *,
    run_id: str,
    case_name: str | None = None,
    workload_name: str | None = None,
    workload_type: str = "Batch Compute",
) -> dict[str, pd.DataFrame]:
    workload = result["workload_input"]
    metrics = result["metrics"]
    region = _clean_text(result.get("region"), "Unknown")
    schedule_df = result["schedule"].copy()
    forecast_df = result["forecast"].copy()
    optimized_df = result["optimized"].copy()
    baseline_df = result["baseline"].copy()

    generated_at = datetime.now(timezone.utc)
    generated_at_label = generated_at.strftime("%Y-%m-%d %H:%M:%S UTC")
    case_name_value = case_name or f"{_format_objective_label(workload.objective)} Optimization - {region}"
    workload_name_value = workload_name or f"Util Run {run_id[-8:]}"

    schedule_df["timestamp"] = pd.to_datetime(schedule_df["timestamp"], errors="coerce")
    optimized_df["timestamp"] = pd.to_datetime(optimized_df["timestamp"], errors="coerce")
    forecast_df["timestamp"] = pd.to_datetime(forecast_df["timestamp"], errors="coerce")
    baseline_df["timestamp"] = pd.to_datetime(baseline_df["timestamp"], errors="coerce")

    selected_df = optimized_df[optimized_df.get("run_flag", 0) == 1].copy()
    eligible_df = schedule_df[schedule_df.get("eligible_flag", 0) == 1].copy()

    selected_totals = _compute_totals(selected_df, int(workload.machine_watts))
    baseline_totals = _compute_totals(baseline_df[baseline_df.get("baseline_run_flag", 0) == 1].copy(), int(workload.machine_watts))
    start_ts = selected_df["timestamp"].min() if not selected_df.empty else None
    end_ts = selected_df["timestamp"].max() if not selected_df.empty else None
    objective = _clean_text(workload.objective, "cost")
    default_cost_weight, default_carbon_weight = _score_weights(objective)
    price_weight = _safe_float(getattr(workload, "price_weight", None))
    carbon_weight = _safe_float(getattr(workload, "carbon_weight", None))
    if objective == "balanced":
        cost_weight = price_weight if price_weight is not None else default_cost_weight
        carbon_weight = carbon_weight if carbon_weight is not None else default_carbon_weight
    else:
        cost_weight, carbon_weight = default_cost_weight, default_carbon_weight
    pricing_source = _clean_text(
        forecast_df.get("pricing_source").dropna().iloc[0]
        if "pricing_source" in forecast_df.columns and not forecast_df.get("pricing_source").dropna().empty
        else "",
        "",
    )
    pricing_region_code = _clean_text(
        forecast_df.get("pricing_region_code").dropna().iloc[0]
        if "pricing_region_code" in forecast_df.columns and not forecast_df.get("pricing_region_code").dropna().empty
        else region,
        region,
    )
    pricing_node = _clean_text(
        forecast_df.get("pricing_node").dropna().iloc[0]
        if "pricing_node" in forecast_df.columns and not forecast_df.get("pricing_node").dropna().empty
        else "",
        "",
    )
    avg_electricity_price_usd_per_kwh = float(
        pd.to_numeric(selected_df.get("price_per_kwh"), errors="coerce").dropna().mean()
    ) if not selected_df.empty and "price_per_kwh" in selected_df.columns else 0.0

    carbon_price_usd_per_ton = get_float_setting("UTIL_CARBON_PRICE_USD_PER_TON", 0.0) or 0.0
    clean_energy_credit_usd = get_float_setting("UTIL_CLEAN_ENERGY_CREDIT_USD", 0.0) or 0.0
    electricity_adder_pct = get_float_setting("UTIL_ELECTRICITY_PRICE_ADDER_PCT", 0.0) or 0.0

    optimized_electricity_cost = _safe_float(metrics.get("optimized_cost")) or selected_totals["cost_usd"]
    baseline_electricity_cost = _safe_float(metrics.get("baseline_cost")) or baseline_totals["cost_usd"]
    optimized_emissions_total = _safe_float(metrics.get("optimized_carbon_kg")) or selected_totals["emissions_total"]
    baseline_emissions_total = _safe_float(metrics.get("baseline_carbon_kg")) or baseline_totals["emissions_total"]

    projected_carbon_cost_usd = optimized_emissions_total * carbon_price_usd_per_ton / 1000.0
    baseline_carbon_cost_usd = baseline_emissions_total * carbon_price_usd_per_ton / 1000.0
    projected_total_cost_usd = optimized_electricity_cost + projected_carbon_cost_usd - clean_energy_credit_usd
    baseline_total_cost_usd = baseline_electricity_cost + baseline_carbon_cost_usd

    cost_savings_usd = baseline_total_cost_usd - projected_total_cost_usd
    cost_savings_pct = (cost_savings_usd / baseline_total_cost_usd * 100.0) if baseline_total_cost_usd else 0.0
    emissions_reduction_total = baseline_emissions_total - optimized_emissions_total
    emissions_reduction_pct = (
        emissions_reduction_total / baseline_emissions_total * 100.0
        if baseline_emissions_total
        else 0.0
    )

    decision_score = (cost_weight * cost_savings_pct) + (carbon_weight * emissions_reduction_pct)
    summary_note = _build_summary_note(
        objective=objective,
        region=region,
        start_local=_format_local_timestamp(start_ts),
        cost_savings_pct=cost_savings_pct,
        emissions_reduction_pct=emissions_reduction_pct,
        price_weight=cost_weight,
        carbon_weight=carbon_weight,
    )

    selected_timestamp_set = set(selected_df["timestamp"].tolist()) if not selected_df.empty else set()
    candidate_windows_df = _build_candidate_windows(
        schedule_df=schedule_df,
        machine_watts=int(workload.machine_watts),
        slots_required=max(len(selected_df), 1),
        deadline=workload.deadline,
        selected_timestamps=selected_timestamp_set,
        cost_weight=cost_weight,
        carbon_weight=carbon_weight,
    )

    recommendation_df = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "case_name": case_name_value,
                "generated_at": generated_at_label,
                "workload_name": workload_name_value,
                "workload_type": workload_type,
                "estimated_energy_kwh": selected_totals["energy_kwh"],
                "estimated_runtime_hours": workload.compute_hours_required,
                "eligible_regions": region,
                "recommended_region": region,
                "recommended_start_time_local": _format_local_timestamp(start_ts),
                "recommended_end_time_local": _format_local_timestamp(end_ts),
                "recommended_start_time_utc": _format_utc_timestamp(start_ts),
                "recommended_end_time_utc": _format_utc_timestamp(end_ts),
                "avg_grid_emissions_gco2_per_kwh": selected_totals["avg_emissions_gco2_per_kwh"],
                "avg_electricity_price_usd_per_kwh": avg_electricity_price_usd_per_kwh,
                "pricing_source": pricing_source,
                "pricing_region_code": pricing_region_code,
                "pricing_node": pricing_node,
                "projected_electricity_cost_usd": optimized_electricity_cost,
                "projected_carbon_cost_usd": projected_carbon_cost_usd,
                "projected_credit_value_usd": clean_energy_credit_usd,
                "projected_total_cost_usd": projected_total_cost_usd,
                "baseline_total_cost_usd": baseline_total_cost_usd,
                "baseline_emissions_total": baseline_emissions_total,
                "optimized_emissions_total": optimized_emissions_total,
                "cost_savings_usd": cost_savings_usd,
                "cost_savings_pct": cost_savings_pct,
                "emissions_reduction_total": emissions_reduction_total,
                "emissions_reduction_pct": emissions_reduction_pct,
                "decision_score": decision_score,
                "primary_objective": objective,
                "notes": summary_note,
            }
        ]
    )

    region_comparison_df = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "case_name": case_name_value,
                "region_name": region,
                "region_code": region,
                "region_eligible": True,
                "eligibility_reason": "Resolved and optimized by current Util run.",
                "candidate_start_time_local": _format_local_timestamp(start_ts),
                "candidate_end_time_local": _format_local_timestamp(end_ts),
                "avg_grid_emissions_gco2_per_kwh": selected_totals["avg_emissions_gco2_per_kwh"],
                "avg_electricity_price_usd_per_kwh": avg_electricity_price_usd_per_kwh,
                "pricing_source": pricing_source,
                "pricing_node": pricing_node,
                "projected_electricity_cost_usd": optimized_electricity_cost,
                "projected_carbon_cost_usd": projected_carbon_cost_usd,
                "projected_credit_value_usd": clean_energy_credit_usd,
                "projected_total_cost_usd": projected_total_cost_usd,
                "estimated_emissions_total": optimized_emissions_total,
                "latency_class": "",
                "distance_class": "",
                "meets_deadline": True,
                "meets_policy_constraints": True,
                "rank_by_total_cost": 1,
                "rank_by_emissions": 1,
                "rank_by_balanced_score": 1,
                "selected_by_optimizer": True,
            }
        ]
    )

    if candidate_windows_df.empty:
        time_window_analysis_df = pd.DataFrame(
            columns=[
                "run_id",
                "case_name",
                "region_name",
                "window_start_local",
                "window_end_local",
                "window_start_utc",
                "window_end_utc",
                "window_duration_hours",
                "forecasted_grid_emissions_gco2_per_kwh",
                "forecasted_electricity_price_usd_per_kwh",
                "projected_electricity_cost_usd",
                "projected_carbon_cost_usd",
                "projected_total_cost_usd",
                "projected_emissions_total",
                "meets_deadline",
                "meets_latency_requirement",
                "meets_policy_constraints",
                "balanced_score",
                "window_rank",
                "selected_window",
            ]
        )
    else:
        time_window_analysis_df = candidate_windows_df.assign(
            run_id=run_id,
            case_name=case_name_value,
            region_name=region,
            window_start_local=candidate_windows_df["window_start"].apply(_format_local_timestamp),
            window_end_local=candidate_windows_df["window_end"].apply(_format_local_timestamp),
            window_start_utc=candidate_windows_df["window_start"].apply(_format_utc_timestamp),
            window_end_utc=candidate_windows_df["window_end"].apply(_format_utc_timestamp),
            projected_carbon_cost_usd=candidate_windows_df["projected_emissions_total"] * carbon_price_usd_per_ton / 1000.0,
            projected_total_cost_usd=(
                candidate_windows_df["projected_electricity_cost_usd"]
                + candidate_windows_df["projected_emissions_total"] * carbon_price_usd_per_ton / 1000.0
                - clean_energy_credit_usd
            ),
        )[
            [
                "run_id",
                "case_name",
                "region_name",
                "window_start_local",
                "window_end_local",
                "window_start_utc",
                "window_end_utc",
                "window_duration_hours",
                "forecasted_grid_emissions_gco2_per_kwh",
                "forecasted_electricity_price_usd_per_kwh",
                "projected_electricity_cost_usd",
                "projected_carbon_cost_usd",
                "projected_total_cost_usd",
                "projected_emissions_total",
                "meets_deadline",
                "meets_latency_requirement",
                "meets_policy_constraints",
                "balanced_score",
                "window_rank",
                "selected_window",
            ]
        ]

    case_comparison_df = pd.DataFrame(
        [
            {
                "case_name": case_name_value,
                "run_id": run_id,
                "created_at": generated_at_label,
                "workload_name": workload_name_value,
                "objective_mode": objective,
                "allowed_regions": region,
                "deadline_hours": "",
                "latency_requirement": "",
                "carbon_price_usd_per_ton": carbon_price_usd_per_ton,
                "clean_energy_credit_usd": clean_energy_credit_usd,
                "electricity_price_adder_pct": electricity_adder_pct,
                "avg_electricity_price_usd_per_kwh": avg_electricity_price_usd_per_kwh,
                "pricing_source": pricing_source,
                "pricing_region_code": pricing_region_code,
                "pricing_node": pricing_node,
                "cost_weight": cost_weight,
                "carbon_weight": carbon_weight,
                "recommended_region": region,
                "recommended_start_time_local": _format_local_timestamp(start_ts),
                "projected_total_cost_usd": projected_total_cost_usd,
                "optimized_emissions_total": optimized_emissions_total,
                "cost_savings_pct": cost_savings_pct,
                "emissions_reduction_pct": emissions_reduction_pct,
                "status": "Recommended",
                "summary_note": summary_note,
            }
        ]
    )

    input_assumptions_df = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "case_name": case_name_value,
                "workload_name": workload_name_value,
                "workload_type": workload_type,
                "input_energy_kwh": selected_totals["energy_kwh"],
                "input_runtime_hours": workload.compute_hours_required,
                "input_gpu_count": "",
                "input_compute_class": "",
                "preferred_regions": region,
                "excluded_regions": "",
                "deadline_hours": "",
                "earliest_start_time": _format_local_timestamp(eligible_df["timestamp"].min() if not eligible_df.empty else None),
                "latest_end_time": _format_local_timestamp(workload.deadline),
                "carbon_price_usd_per_ton": carbon_price_usd_per_ton,
                "clean_energy_credit_usd": clean_energy_credit_usd,
                "coupon_or_incentive_name": "",
                "electricity_adder_pct": electricity_adder_pct,
                "pricing_source": pricing_source,
                "pricing_region_code": pricing_region_code,
                "pricing_node": pricing_node,
                "policy_mode": "",
                "cost_weight": cost_weight,
                "carbon_weight": carbon_weight,
                "latency_requirement": "",
                "data_residency_requirement": "",
                "renewable_preference_enabled": False,
                "fallback_allowed": True,
                "baseline_definition": "Run at earliest eligible intervals.",
                "optimizer_version": "util-streamlit-v1",
            }
        ]
    )

    run_summary_df = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "case_name": case_name_value,
                "generated_at": generated_at_label,
                "workload_name": workload_name_value,
                "recommended_region": region,
                "recommended_start_time_local": _format_local_timestamp(start_ts),
                "avg_electricity_price_usd_per_kwh": avg_electricity_price_usd_per_kwh,
                "pricing_source": pricing_source,
                "pricing_node": pricing_node,
                "projected_total_cost_usd": projected_total_cost_usd,
                "optimized_emissions_total": optimized_emissions_total,
                "cost_savings_pct": cost_savings_pct,
                "emissions_reduction_pct": emissions_reduction_pct,
                "status": "Recommended",
                "summary_note": summary_note,
            }
        ]
    )

    return {
        EXPORT_FILENAMES["recommendation"]: recommendation_df,
        EXPORT_FILENAMES["region_comparison"]: region_comparison_df,
        EXPORT_FILENAMES["time_window_analysis"]: time_window_analysis_df,
        EXPORT_FILENAMES["case_comparison"]: case_comparison_df,
        EXPORT_FILENAMES["input_assumptions"]: input_assumptions_df,
        EXPORT_FILENAMES["run_summary"]: run_summary_df,
    }


def write_export_frames(
    export_dir: str | Path,
    frames: dict[str, pd.DataFrame],
) -> list[Path]:
    export_path = Path(export_dir)
    export_path.mkdir(parents=True, exist_ok=True)

    written_paths: list[Path] = []
    for filename, frame in frames.items():
        path = export_path / filename
        frame.to_csv(path, index=False)
        written_paths.append(path)

    return written_paths


def generate_export_package(
    result: dict[str, Any],
    *,
    export_root: str | Path,
    enable_cloud_upload: bool = False,
    run_id: str | None = None,
    case_name: str | None = None,
    workload_name: str | None = None,
    workload_type: str = "Batch Compute",
) -> dict[str, Any]:
    resolved_run_id = run_id or generate_run_id()
    export_dir = Path(export_root) / resolved_run_id

    frames = build_export_frames(
        result,
        run_id=resolved_run_id,
        case_name=case_name,
        workload_name=workload_name,
        workload_type=workload_type,
    )
    files = write_export_frames(export_dir, frames)
    if enable_cloud_upload:
        cloud_uploads = upload_run_outputs(resolved_run_id, files)
    else:
        cloud_uploads = {
            "configured": False,
            "message": "",
            "bucket_name": None,
            "files": [],
            "status_detail": "",
            "env_path": "",
            "failure_reason": None,
            "error_detail": None,
            "region_name": None,
            "debug_detail": "",
        }

    return {
        "run_id": resolved_run_id,
        "export_dir": str(export_dir),
        "files": [str(path) for path in files],
        "cloud_save_enabled": enable_cloud_upload,
        "cloud_outputs": cloud_uploads["files"],
        "cloud_storage_configured": cloud_uploads["configured"],
        "cloud_message": cloud_uploads["message"],
        "s3_bucket_name": cloud_uploads["bucket_name"],
        "cloud_region_name": cloud_uploads["region_name"],
        "cloud_status_detail": cloud_uploads["status_detail"],
        "cloud_env_path": cloud_uploads["env_path"],
        "cloud_failure_reason": cloud_uploads["failure_reason"],
        "cloud_error_detail": cloud_uploads["error_detail"],
    }
