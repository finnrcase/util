from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from src.analytics import (
    analytics_file_exists,
    clear_analytics_data,
    filter_analytics_data,
    load_analytics_data,
    summarize_analytics,
)


ADMIN_PASSWORD = "utiladmin"


def init_admin_state() -> None:
    if "admin_unlocked" not in st.session_state:
        st.session_state["admin_unlocked"] = False

    if "admin_password_error" not in st.session_state:
        st.session_state["admin_password_error"] = ""

    if "confirm_clear_analytics" not in st.session_state:
        st.session_state["confirm_clear_analytics"] = False

    if "last_successful_api_pull_time" not in st.session_state:
        st.session_state["last_successful_api_pull_time"] = None

    if "watttime_token_available" not in st.session_state:
        st.session_state["watttime_token_available"] = None

    if "last_analytics_log_message" not in st.session_state:
        st.session_state["last_analytics_log_message"] = None

    dev_mode = os.getenv("DEV_MODE", "").strip().lower() in {"1", "true", "yes", "on"}
    if dev_mode:
        st.session_state["admin_unlocked"] = True


def render_admin_access_panel() -> bool:
    init_admin_state()

    with st.sidebar.expander("Internal Admin", expanded=st.session_state["admin_unlocked"]):
        st.caption("Private developer dashboard access.")

        if st.session_state["admin_unlocked"]:
            st.success("Admin dashboard unlocked for this session.")
            if st.button("Lock Admin Dashboard", key="sidebar_lock_admin_dashboard"):
                st.session_state["admin_unlocked"] = False
                st.session_state["admin_password_error"] = ""
                st.session_state["confirm_clear_analytics"] = False
                st.rerun()
        else:
            password_value = st.text_input(
                "Admin password",
                type="password",
                key="admin_password_input",
                label_visibility="collapsed",
                placeholder="Admin password",
            )
            if st.button("Unlock Admin Dashboard", key="unlock_admin_dashboard"):
                if password_value == ADMIN_PASSWORD:
                    st.session_state["admin_unlocked"] = True
                    st.session_state["admin_password_error"] = ""
                    st.rerun()
                else:
                    st.session_state["admin_password_error"] = "Incorrect password."

            if st.session_state["admin_password_error"]:
                st.error(st.session_state["admin_password_error"])

    return bool(st.session_state["admin_unlocked"])


def build_run_analytics_record(
    *,
    result: dict[str, Any],
    run_type: str,
    schedule_mode_label: str,
    forecast_mode_label: str,
    api_mode: str,
    notes: str = "",
) -> dict[str, Any]:
    workload = result["workload_input"]
    metrics = result["metrics"]
    schedule = result["schedule"].copy()

    selected_intervals = int(schedule["run_flag"].fillna(0).sum()) if "run_flag" in schedule else 0
    eligible_intervals = int(schedule["eligible_flag"].fillna(0).sum()) if "eligible_flag" in schedule else 0

    best_start_time = None
    if "run_flag" in schedule.columns:
        selected_rows = schedule[schedule["run_flag"] == 1].copy()
        if not selected_rows.empty and "timestamp" in selected_rows.columns:
            best_start_time = pd.to_datetime(selected_rows["timestamp"], errors="coerce").min()

    return {
        "timestamp": datetime.now(),
        "run_type": run_type,
        "compute_hours": getattr(workload, "compute_hours_required", None),
        "region": result.get("region"),
        "zip_code": getattr(workload, "zip_code", None),
        "schedule_mode": schedule_mode_label,
        "objective_mode": getattr(workload, "objective", None),
        "machine_watts": getattr(workload, "machine_watts", None),
        "machine_kw": (
            float(workload.machine_watts) / 1000.0
            if getattr(workload, "machine_watts", None) is not None
            else None
        ),
        "baseline_emissions": metrics.get("baseline_carbon_kg"),
        "optimized_emissions": metrics.get("optimized_carbon_kg"),
        "carbon_saved": metrics.get("carbon_savings_kg"),
        "carbon_reduction_pct": metrics.get("carbon_reduction_pct"),
        "baseline_cost": metrics.get("baseline_cost"),
        "optimized_cost": metrics.get("optimized_cost"),
        "cost_saved": metrics.get("cost_savings"),
        "selected_interval_count": selected_intervals,
        "eligible_interval_count": eligible_intervals,
        "best_start_time": best_start_time,
        "deadline": getattr(workload, "deadline", None),
        "api_mode": api_mode,
        "forecast_mode": forecast_mode_label,
        "notes": notes,
    }


def _render_status_item(label: str, status: str, detail: str) -> None:
    status_color = {
        "Healthy": "#6ee7b7",
        "Warning": "#fbbf24",
        "Missing": "#f87171",
        "Disabled": "#94a3b8",
    }.get(status, "#cbd5e1")
    st.markdown(
        f"""
        <div class="util-card" style="padding: 1rem 1.1rem; min-height: 112px;">
            <div style="display:flex; justify-content:space-between; align-items:center; gap:12px;">
                <strong>{label}</strong>
                <span style="color:{status_color}; font-weight:700;">{status}</span>
            </div>
            <div style="margin-top:0.55rem; color:var(--util-muted); font-size:0.95rem;">{detail}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_metric_grid(summary: dict[str, float | int | None]) -> None:
    row1 = st.columns(5)
    row1[0].metric("Total Logged Runs", f"{summary['total_logged_runs']:,}")
    row1[1].metric("Total Real Runs", f"{summary['total_real_runs']:,}")
    row1[2].metric("Total Test Runs", f"{summary['total_test_runs']:,}")
    row1[3].metric("Compute Hours Optimized", f"{summary['total_compute_hours']:.2f}")
    row1[4].metric("Total Carbon Saved", f"{summary['total_carbon_saved']:.2f} kg")

    row2 = st.columns(5)
    row2[0].metric("Total Cost Saved", f"${summary['total_cost_saved']:.2f}")
    row2[1].metric("Avg Carbon Saved / Real Run", f"{summary['avg_carbon_saved_per_real_run']:.2f} kg")
    row2[2].metric("Avg Cost Saved / Real Run", f"${summary['avg_cost_saved_per_real_run']:.2f}")
    row2[3].metric("Avg Carbon Reduction", f"{summary['avg_carbon_reduction_pct']:.2f}%")
    row2[4].metric("Avg Selected Intervals", f"{summary['avg_selected_interval_count']:.2f}")


def _build_time_series_chart(
    df: pd.DataFrame,
    y_column: str,
    title: str,
    color: str,
    mark_type: str = "line",
) -> alt.Chart:
    if mark_type == "bar":
        chart = alt.Chart(df).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color=color)
    else:
        chart = alt.Chart(df).mark_line(point=True, strokeWidth=3, color=color)

    return chart.encode(
        x=alt.X("timestamp:T", title="Time"),
        y=alt.Y(f"{y_column}:Q", title=title),
        tooltip=["timestamp:T", alt.Tooltip(f"{y_column}:Q", format=".2f")],
    ).properties(height=280, title=title)


def _build_count_chart(df: pd.DataFrame, column: str, title: str, color: str) -> alt.Chart:
    counts = (
        df[column]
        .fillna("Unknown")
        .value_counts()
        .rename_axis(column)
        .reset_index(name="count")
    )
    return alt.Chart(counts).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, color=color).encode(
        x=alt.X(f"{column}:N", title=title),
        y=alt.Y("count:Q", title="Runs"),
        tooltip=[column, "count:Q"],
    ).properties(height=280, title=title)


def _render_analytics_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("Analytics Filters")

    col1, col2, col3, col4 = st.columns([1.2, 1.2, 1, 1])

    with col1:
        include_test_runs = st.checkbox(
            "Include test runs?",
            value=False,
            key="admin_include_test_runs",
        )

    min_date = None
    max_date = None
    if not df.empty and df["timestamp"].notna().any():
        min_date = df["timestamp"].min().date()
        max_date = df["timestamp"].max().date()

    with col2:
        if min_date and max_date:
            date_range = st.date_input(
                "Date range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                key="admin_date_range",
            )
        else:
            date_range = ()
            st.caption("Date range will appear after analytics are logged.")

    regions = ["All"]
    schedule_modes = ["All"]
    if not df.empty:
        regions += sorted([value for value in df["region"].dropna().unique().tolist() if value])
        schedule_modes += sorted(
            [value for value in df["schedule_mode"].dropna().unique().tolist() if value]
        )

    with col3:
        selected_region = st.selectbox("Region", regions, key="admin_region_filter")
    with col4:
        selected_schedule_mode = st.selectbox(
            "Schedule mode",
            schedule_modes,
            key="admin_schedule_mode_filter",
        )

    start_date = None
    end_date = None
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range

    return filter_analytics_data(
        df,
        include_test_runs=include_test_runs,
        start_date=start_date,
        end_date=end_date,
        region=selected_region,
        schedule_mode=selected_schedule_mode,
    )


def _render_savings_graphs(filtered_df: pd.DataFrame) -> None:
    st.subheader("Savings / Impact Graphs")

    if filtered_df.empty:
        st.info("No analytics data matches the current filters yet.")
        return

    charts_df = filtered_df.sort_values("timestamp").copy()
    charts_df["run_type_label"] = charts_df["run_type"].fillna("Unknown")
    charts_df["cumulative_carbon_saved"] = charts_df["carbon_saved"].fillna(0).cumsum()

    g1, g2 = st.columns(2)
    with g1:
        st.altair_chart(
            _build_time_series_chart(
                charts_df,
                y_column="carbon_saved",
                title="Carbon Saved Over Time",
                color="#7b6dff",
                mark_type="bar",
            ),
            use_container_width=True,
        )
    with g2:
        st.altair_chart(
            _build_time_series_chart(
                charts_df,
                y_column="cumulative_carbon_saved",
                title="Cumulative Carbon Saved",
                color="#6ee7b7",
            ),
            use_container_width=True,
        )

    g3, g4 = st.columns(2)
    with g3:
        if charts_df["cost_saved"].dropna().empty:
            st.info("Cost data is not available in the current filtered runs yet.")
        else:
            st.altair_chart(
                _build_time_series_chart(
                    charts_df.dropna(subset=["cost_saved"]),
                    y_column="cost_saved",
                    title="Cost Saved Over Time",
                    color="#f59e0b",
                    mark_type="bar",
                ),
                use_container_width=True,
            )
    with g4:
        st.altair_chart(
            _build_count_chart(
                charts_df,
                column="run_type_label",
                title="Real vs Test Runs",
                color="#a77bff",
            ),
            use_container_width=True,
        )

    g5, g6 = st.columns(2)
    with g5:
        if charts_df["carbon_reduction_pct"].dropna().empty:
            st.info("Carbon reduction trend will appear after runs include reduction data.")
        else:
            st.altair_chart(
                _build_time_series_chart(
                    charts_df.dropna(subset=["carbon_reduction_pct"]),
                    y_column="carbon_reduction_pct",
                    title="Carbon Reduction % Over Time",
                    color="#38bdf8",
                ),
                use_container_width=True,
            )
    with g6:
        group_column = "region" if charts_df["region"].dropna().any() else "schedule_mode"
        title = "Runs by Region" if group_column == "region" else "Runs by Schedule Mode"
        st.altair_chart(
            _build_count_chart(
                charts_df,
                column=group_column,
                title=title,
                color="#fb7185",
            ),
            use_container_width=True,
        )


def _render_system_status(
    *,
    analytics_path: str | Path,
    current_context: dict[str, Any],
) -> None:
    st.subheader("System Status")

    credentials_configured = bool(os.getenv("WATTTIME_USERNAME")) and bool(os.getenv("WATTTIME_PASSWORD"))
    token_available = st.session_state.get("watttime_token_available")
    last_pull = st.session_state.get("last_successful_api_pull_time")
    zip_mapping_available = Path(current_context["zip_mapping_path"]).exists()
    logging_enabled = bool(current_context.get("analytics_logging_enabled"))
    analytics_exists = analytics_file_exists(analytics_path)
    forecast_mode = current_context.get("forecast_mode") or "Unknown"
    schedule_mode = current_context.get("schedule_mode") or "Unknown"
    objective_mode = current_context.get("objective_mode") or "Unknown"
    app_mode = current_context.get("app_mode") or "Unknown"

    statuses = [
        (
            "WattTime API Configured",
            "Healthy" if credentials_configured else "Missing",
            "Credentials detected in environment." if credentials_configured else "WATTTIME_USERNAME / WATTTIME_PASSWORD missing.",
        ),
        (
            "WattTime Auth / Token",
            "Healthy" if token_available is True else "Warning" if credentials_configured else "Missing",
            "Recent live run authenticated successfully." if token_available is True else "Credentials exist, but no successful token check is recorded in this session." if credentials_configured else "Authentication cannot work without credentials.",
        ),
        (
            "Last Successful API Pull",
            "Healthy" if last_pull else "Warning",
            last_pull if last_pull else "No successful live forecast pull recorded in this session yet.",
        ),
        (
            "ZIP / Region Mapping",
            "Healthy" if zip_mapping_available else "Missing",
            str(current_context["zip_mapping_path"]),
        ),
        (
            "Analytics Logging",
            "Healthy" if logging_enabled else "Disabled",
            "Current optimizer run will be logged." if logging_enabled else "Current optimizer run will not be logged.",
        ),
        (
            "Analytics File",
            "Healthy" if analytics_exists else "Warning",
            str(analytics_path),
        ),
        (
            "Forecast Mode",
            "Healthy",
            str(forecast_mode),
        ),
        (
            "Schedule Mode",
            "Healthy",
            str(schedule_mode),
        ),
        (
            "Objective",
            "Healthy",
            str(objective_mode),
        ),
        (
            "App Environment",
            "Healthy" if str(app_mode).lower() in {"live", "dev"} else "Warning",
            str(app_mode),
        ),
    ]

    columns = st.columns(2)
    for index, item in enumerate(statuses):
        with columns[index % 2]:
            _render_status_item(*item)


def _render_data_table(filtered_df: pd.DataFrame, analytics_path: str | Path) -> None:
    st.subheader("Analytics Data")

    if filtered_df.empty:
        st.info("No logged runs are available for the current filters.")
        return

    recent_df = filtered_df.sort_values("timestamp", ascending=False).copy()
    display_columns = [
        "timestamp",
        "run_type",
        "compute_hours",
        "region",
        "carbon_saved",
        "cost_saved",
        "carbon_reduction_pct",
        "schedule_mode",
    ]
    display_df = recent_df[[column for column in display_columns if column in recent_df.columns]].copy()

    st.dataframe(display_df, use_container_width=True)
    st.download_button(
        "Export Analytics CSV",
        data=recent_df.to_csv(index=False).encode("utf-8"),
        file_name=Path(analytics_path).name,
        mime="text/csv",
        key="admin_export_analytics_csv",
    )


def _render_management_tools(analytics_path: str | Path) -> None:
    st.subheader("Admin Tools")

    tool_col1, tool_col2, tool_col3 = st.columns(3)

    with tool_col1:
        if st.button("Clear Analytics Data", key="clear_analytics_data_start"):
            st.session_state["confirm_clear_analytics"] = True

        if st.session_state.get("confirm_clear_analytics"):
            st.warning("Click confirm to permanently clear the local analytics CSV.")
            if st.button("Confirm Clear Analytics", key="confirm_clear_analytics_button"):
                clear_analytics_data(analytics_path)
                st.session_state["confirm_clear_analytics"] = False
                st.success("Analytics data cleared.")
                st.rerun()

    with tool_col2:
        if st.button("Lock Admin Dashboard", key="admin_tab_lock_dashboard"):
            st.session_state["admin_unlocked"] = False
            st.session_state["confirm_clear_analytics"] = False
            st.rerun()

    with tool_col3:
        st.caption("Analytics is stored locally and persists across app restarts.")


def render_admin_dashboard(
    *,
    analytics_path: str | Path,
    current_context: dict[str, Any],
) -> None:
    analytics_df = load_analytics_data(analytics_path)
    filtered_df = _render_analytics_filters(analytics_df)

    st.subheader("Impact Summary Metrics")
    summary = summarize_analytics(filtered_df)
    _render_metric_grid(summary)

    _render_savings_graphs(filtered_df)
    _render_system_status(analytics_path=analytics_path, current_context=current_context)
    _render_data_table(filtered_df, analytics_path)
    _render_management_tools(analytics_path)
