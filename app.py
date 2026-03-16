import streamlit as st
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import altair as alt
from PIL import Image

from src.inputs import WorkloadInput
from src.pipeline import run_util_pipeline

# ---------------------------------------------------
# Paths
# ---------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"

LOGO_PATH = PROJECT_ROOT / "assets" / "logo" / "util_logo.png"
logo = Image.open(LOGO_PATH)

ZIP_PATH = DATA_DIR / "zip_to_region_sample.csv"
CARBON_PATH = DATA_DIR / "sample_carbon_forecast.csv"
PRICE_PATH = DATA_DIR / "sample_price_forecast.csv"

# ---------------------------------------------------
# Page Setup
# ---------------------------------------------------

st.set_page_config(
    page_title="Util",
    page_icon="⚡",
    layout="wide"
)

st.markdown(
    """
    <style>

    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');

    html, body, [class*="css"]  {
        font-family: 'Inter', sans-serif;
    }

    h1, h2, h3 {
        font-weight: 600;
        letter-spacing: -0.02em;
    }

    .stTabs [data-baseweb="tab"] {
        font-weight: 500;
        font-size: 15px;
    }

    </style>
    """,
    unsafe_allow_html=True
)

# ---------------------------------------------------
# Custom Styling
# ---------------------------------------------------

st.markdown(
    """
    <style>
    .stApp {
        background: linear-gradient(180deg, #0a0a0f 0%, #111118 100%);
        color: #f3f3f7;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1300px;
    }

    h1, h2, h3 {
        color: #f7f5ff;
        letter-spacing: 0.2px;
    }

    .util-subtext {
        color: #b8b8c7;
        font-size: 1rem;
        margin-bottom: 1rem;
    }

    .util-card {
        background: rgba(24, 24, 34, 0.92);
        border: 1px solid rgba(141, 95, 255, 0.20);
        border-radius: 18px;
        padding: 1.1rem 1.2rem;
        box-shadow: 0 0 0 1px rgba(255,255,255,0.02);
        margin-bottom: 1rem;
    }

    .util-metric-label {
        color: #b7b7c9;
        font-size: 0.9rem;
        margin-bottom: 0.25rem;
    }

    .util-metric-value {
        color: #ffffff;
        font-size: 1.8rem;
        font-weight: 700;
        line-height: 1.1;
    }

    .util-metric-delta {
        color: #a78bfa;
        font-size: 0.95rem;
        margin-top: 0.35rem;
    }

    div[data-baseweb="tab-list"] {
        gap: 0.5rem;
    }

    button[data-baseweb="tab"] {
        background: #12121a !important;
        color: #bdbdd3 !important;
        border-radius: 10px;
        padding: 0.55rem 1rem;
        border: 1px solid rgba(141, 95, 255, 0.18);
    }

    button[data-baseweb="tab"][aria-selected="true"] {
        background: #8b5cf6 !important;
        color: #ffffff !important;
        border: none !important;
    }

    div.stButton > button {
        background: linear-gradient(90deg, #7c3aed 0%, #8b5cf6 100%);
        color: white;
        border: none;
        border-radius: 12px;
        font-weight: 600;
        padding: 0.6rem 1rem;
    }

    div.stButton > button:hover {
        background: linear-gradient(90deg, #6d28d9 0%, #7c3aed 100%);
        color: white;
    }

    div[data-testid="stMetric"] {
        background: rgba(24, 24, 34, 0.92);
        border: 1px solid rgba(141, 95, 255, 0.20);
        padding: 1rem;
        border-radius: 18px;
    }

    div[data-testid="stDataFrame"] {
        border-radius: 16px;
        overflow: hidden;
    }

    .util-pill {
        display: inline-block;
        padding: 0.35rem 0.7rem;
        border-radius: 999px;
        background: rgba(141, 95, 255, 0.15);
        color: #d7c7ff;
        font-size: 0.85rem;
        border: 1px solid rgba(141, 95, 255, 0.25);
        margin-right: 0.4rem;
        margin-bottom: 0.4rem;
    }

    .util-good-pill {
        display: inline-block;
        padding: 0.35rem 0.7rem;
        border-radius: 999px;
        background: rgba(34, 197, 94, 0.12);
        color: #bbf7d0;
        font-size: 0.85rem;
        border: 1px solid rgba(34, 197, 94, 0.25);
        margin-right: 0.4rem;
        margin-bottom: 0.4rem;
    }

    .util-warning-pill {
        display: inline-block;
        padding: 0.35rem 0.7rem;
        border-radius: 999px;
        background: rgba(245, 158, 11, 0.12);
        color: #fde68a;
        font-size: 0.85rem;
        border: 1px solid rgba(245, 158, 11, 0.25);
        margin-right: 0.4rem;
        margin-bottom: 0.4rem;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ---------------------------------------------------
# Helpers
# ---------------------------------------------------

def render_metric_card(label: str, value: str, delta: str | None = None):
    delta_html = f'<div class="util-metric-delta">{delta}</div>' if delta else ""
    st.markdown(
        f"""
        <div class="util-card">
            <div class="util-metric-label">{label}</div>
            <div class="util-metric-value">{value}</div>
            {delta_html}
        </div>
        """,
        unsafe_allow_html=True
    )


def infer_interval_minutes(df: pd.DataFrame) -> float:
    if len(df) < 2:
        return 60.0

    ts = pd.to_datetime(df["timestamp"]).sort_values()
    diffs = ts.diff().dropna()

    if diffs.empty:
        return 60.0

    return diffs.dt.total_seconds().median() / 60


def build_forecast_display_df(
    forecast_df: pd.DataFrame,
    optimized_df: pd.DataFrame | None = None
) -> pd.DataFrame:
    df = forecast_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour_label"] = df["timestamp"].dt.strftime("%b %d, %I:%M %p")

    if optimized_df is not None:
        run_lookup = optimized_df[["timestamp", "run_flag", "eligible_flag"]].copy()
        run_lookup["timestamp"] = pd.to_datetime(run_lookup["timestamp"])
        df = df.merge(run_lookup, on="timestamp", how="left")
    else:
        df["run_flag"] = 0
        df["eligible_flag"] = 0

    df["run_flag"] = df["run_flag"].fillna(0).astype(int)
    df["eligible_flag"] = df["eligible_flag"].fillna(0).astype(int)

    df["recommended_action"] = df["run_flag"].map({1: "Run", 0: "Wait"})
    return df


def build_timeline_df(schedule_df: pd.DataFrame) -> pd.DataFrame:
    df = schedule_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["Hour"] = df["timestamp"].dt.strftime("%b %d, %I:%M %p")

    def label_action(action: str) -> str:
        if action == "Run":
            return "🟣 Run"
        if action == "Wait":
            return "⚫ Wait"
        return "◻ Unavailable"

    df["Action"] = df["recommended_action"].apply(label_action)

    columns = [
        "Hour",
        "Action",
        "price_per_kwh",
        "carbon_g_per_kwh",
        "eligible_flag",
        "run_flag",
    ]
    return df[columns].rename(
        columns={
            "price_per_kwh": "Price ($/kWh)",
            "carbon_g_per_kwh": "Carbon (g/kWh)",
            "eligible_flag": "Eligible",
            "run_flag": "Run Flag",
        }
    )


def build_selected_schedule_df(schedule_df: pd.DataFrame) -> pd.DataFrame:
    df = schedule_df[schedule_df["run_flag"] == 1].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["Time"] = df["timestamp"].dt.strftime("%b %d, %I:%M %p")

    return df[
        ["Time", "recommended_action", "price_per_kwh", "carbon_g_per_kwh"]
    ].rename(
        columns={
            "recommended_action": "Action",
            "price_per_kwh": "Price ($/kWh)",
            "carbon_g_per_kwh": "Carbon (g/kWh)",
        }
    )


def build_run_hours_summary(schedule_df: pd.DataFrame) -> str:
    run_rows = schedule_df[schedule_df["run_flag"] == 1].copy()
    if run_rows.empty:
        return "No run hours selected."

    run_rows["timestamp"] = pd.to_datetime(run_rows["timestamp"])
    labels = run_rows["timestamp"].dt.strftime("%b %d %I:%M %p").tolist()

    if len(labels) > 12:
        return " • ".join(labels[:12]) + f" • ... ({len(labels)} selected intervals)"
    return " • ".join(labels)


def build_run_window_summary(schedule_df: pd.DataFrame) -> dict:
    run_rows = schedule_df[schedule_df["run_flag"] == 1].copy()

    if run_rows.empty:
        return {
            "start": "N/A",
            "end": "N/A",
            "intervals": 0
        }

    run_rows["timestamp"] = pd.to_datetime(run_rows["timestamp"])

    return {
        "start": run_rows["timestamp"].min().strftime("%b %d, %I:%M %p"),
        "end": run_rows["timestamp"].max().strftime("%b %d, %I:%M %p"),
        "intervals": len(run_rows)
    }


def compute_schedule_totals(
    schedule_like_df: pd.DataFrame,
    machine_watts: int
) -> dict:
    df = schedule_like_df.copy()
    if df.empty:
        return {"cost": 0.0, "carbon_kg": 0.0, "energy_kwh": 0.0}

    interval_minutes = infer_interval_minutes(df)
    interval_hours = interval_minutes / 60
    power_kw = machine_watts / 1000

    energy_per_row_kwh = power_kw * interval_hours
    total_energy_kwh = energy_per_row_kwh * len(df)

    total_cost = (df["price_per_kwh"] * energy_per_row_kwh).sum()
    total_carbon_kg = ((df["carbon_g_per_kwh"] * energy_per_row_kwh).sum()) / 1000

    return {
        "cost": float(total_cost),
        "carbon_kg": float(total_carbon_kg),
        "energy_kwh": float(total_energy_kwh)
    }


def build_run_now_comparison(
    optimized_df: pd.DataFrame,
    machine_watts: int
) -> dict:
    df = optimized_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    selected_df = df[df["run_flag"] == 1].copy()
    eligible_df = df[df["eligible_flag"] == 1].copy().sort_values("timestamp")

    slots_required = len(selected_df)
    run_now_df = eligible_df.head(slots_required).copy()

    optimized_totals = compute_schedule_totals(selected_df, machine_watts)
    run_now_totals = compute_schedule_totals(run_now_df, machine_watts)

    return {
        "run_now_df": run_now_df,
        "optimized_df": selected_df,
        "run_now_cost": run_now_totals["cost"],
        "optimized_cost": optimized_totals["cost"],
        "run_now_carbon_kg": run_now_totals["carbon_kg"],
        "optimized_carbon_kg": optimized_totals["carbon_kg"],
        "cost_saved_vs_now": run_now_totals["cost"] - optimized_totals["cost"],
        "carbon_saved_vs_now_kg": run_now_totals["carbon_kg"] - optimized_totals["carbon_kg"],
    }


def render_status_pills(
    forecast_mode_label: str,
    schedule_mode_label: str,
    region: str,
    forecast_df: pd.DataFrame
):
    interval_minutes = infer_interval_minutes(forecast_df)
    forecast_min = pd.to_datetime(forecast_df["timestamp"]).min()
    forecast_max = pd.to_datetime(forecast_df["timestamp"]).max()

    source_label = "WattTime API" if forecast_mode_label == "Live Carbon" else "Sample CSV Data"
    source_css = "util-good-pill" if forecast_mode_label == "Live Carbon" else "util-warning-pill"

    forecast_region_used = None
    forecast_access_mode = None

    if "forecast_region_used" in forecast_df.columns:
        non_null_used = forecast_df["forecast_region_used"].dropna()
        if not non_null_used.empty:
            forecast_region_used = non_null_used.iloc[0]

    if "forecast_access_mode" in forecast_df.columns:
        non_null_mode = forecast_df["forecast_access_mode"].dropna()
        if not non_null_mode.empty:
            forecast_access_mode = non_null_mode.iloc[0]

    extra_pills = ""
    if forecast_region_used:
        extra_pills += f'<span class="util-pill">Forecast Region Used: {forecast_region_used}</span>'

    if forecast_access_mode == "preview_fallback":
        extra_pills += '<span class="util-warning-pill">Access Mode: Preview Fallback</span>'
    elif forecast_access_mode == "direct_region":
        extra_pills += '<span class="util-good-pill">Access Mode: Direct Region</span>'

    st.markdown(
        f"""
        <span class="{source_css}">Source: {source_label}</span>
        <span class="util-pill">Resolved Region: {region}</span>
        <span class="util-pill">Forecast Mode: {forecast_mode_label}</span>
        <span class="util-pill">Schedule Mode: {schedule_mode_label}</span>
        <span class="util-pill">Granularity: {interval_minutes:.0f} min</span>
        <span class="util-pill">Forecast Window: {forecast_min.strftime("%b %d %I:%M %p")} → {forecast_max.strftime("%b %d %I:%M %p")}</span>
        {extra_pills}
        """,
        unsafe_allow_html=True
    )


def build_carbon_chart(display_df: pd.DataFrame) -> alt.Chart:
    chart_df = display_df.copy()
    chart_df["timestamp"] = pd.to_datetime(chart_df["timestamp"])
    chart_df["Selected"] = chart_df["run_flag"].apply(lambda x: "Selected" if x == 1 else "Not Selected")

    base = alt.Chart(chart_df).encode(
        x=alt.X("timestamp:T", title="Time"),
        y=alt.Y("carbon_g_per_kwh:Q", title="Carbon Intensity (g/kWh)")
    )

    line = base.mark_line(color="#8b5cf6", strokeWidth=2).encode(
        tooltip=[
            alt.Tooltip("timestamp:T", title="Time"),
            alt.Tooltip("carbon_g_per_kwh:Q", title="Carbon (g/kWh)", format=".1f"),
            alt.Tooltip("price_per_kwh:Q", title="Price ($/kWh)", format=".3f"),
            alt.Tooltip("Selected:N", title="Selection")
        ]
    )

    selected_points = base.transform_filter(
        alt.datum.run_flag == 1
    ).mark_circle(
        size=90,
        color="#22c55e"
    )

    return (line + selected_points).properties(height=350)


# ---------------------------------------------------
# Session State Defaults
# ---------------------------------------------------

if "estimated_power_watts" not in st.session_state:
    st.session_state["estimated_power_watts"] = 300

if "optimizer_machine_watts" not in st.session_state:
    st.session_state["optimizer_machine_watts"] = 300

if "result" not in st.session_state:
    st.session_state["result"] = None

if "last_forecast_mode_label" not in st.session_state:
    st.session_state["last_forecast_mode_label"] = "Demo"

if "last_schedule_mode_label" not in st.session_state:
    st.session_state["last_schedule_mode_label"] = "Flexible"

# ---------------------------------------------------
# Header
# ---------------------------------------------------

col_logo, col_title = st.columns([1, 6])

with col_logo:
    st.image(logo, width=80)

with col_title:
    st.markdown("# Util")

st.markdown(
    """
    Compute scheduling software that minimizes **electricity cost** or **carbon emissions** by choosing the best times to run workloads.
    """
)

# ---------------------------------------------------
# Tabs
# ---------------------------------------------------

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Optimizer",
    "Savings Analysis",
    "Forecast Signals",
    "Run Timeline",
    "Power Estimator",
    "About Util"
])

# ====================================================
# TAB 1 — OPTIMIZER
# ====================================================

with tab1:
    st.header("Workload Optimization")

    col_input, col_output = st.columns([1, 2], gap="large")

    with col_input:
        st.markdown('<div class="util-card">', unsafe_allow_html=True)
        st.subheader("Inputs")

        zip_code = st.text_input("ZIP Code", "93106")

        compute_hours = st.number_input(
            "Compute Hours Required",
            min_value=1,
            max_value=72,
            value=8,
            step=1
        )

        objective = st.selectbox(
            "Optimization Objective",
            ["carbon", "cost"]
        )

        forecast_mode_label = st.radio(
            "Forecast Source",
            ["Demo", "Live Carbon"],
            horizontal=True
        )
        carbon_estimation_mode_label = st.radio(
            "Carbon Estimate Type",
            ["Forecast Only", "Forecast + Historical Expectation"],
            horizontal=True
        )

        historical_days = st.slider(
            "Historical Lookback (days)",
            min_value=1,
            max_value=14,
            value=7,
            step=1
        )
        schedule_mode_label = st.radio(
            "Scheduling Strategy",
            ["Flexible", "Continuous Block"],
            horizontal=True
        )

        if st.session_state["optimizer_machine_watts"] != st.session_state["estimated_power_watts"]:
            st.session_state["optimizer_machine_watts"] = st.session_state["estimated_power_watts"]

        machine_watts = st.number_input(
            "Machine Wattage (Watts)",
            min_value=50,
            max_value=500000,
            step=10,
            key="optimizer_machine_watts"
        )

        default_deadline = datetime.now() + timedelta(hours=24)
        deadline = st.datetime_input(
            "Deadline",
            value=default_deadline
        )

        run_button = st.button("Run Optimization")

        st.markdown("</div>", unsafe_allow_html=True)

        st.caption(
            "Tip: use the Power Estimator tab to generate a machine wattage estimate."
        )
        carbon_estimation_mode = (
                    "forecast_plus_historical_expectation"
                    if carbon_estimation_mode_label == "Forecast + Historical Expectation"
                    else "forecast_only"
                )  
    with col_output:
        if run_button:
            st.session_state["result"] = None
            try:
                forecast_mode = "live_carbon" if forecast_mode_label == "Live Carbon" else "demo"
                schedule_mode = "block" if schedule_mode_label == "Continuous Block" else "flexible"

                workload = WorkloadInput(
                    zip_code=zip_code,
                    compute_hours_required=int(compute_hours),
                    deadline=deadline,
                    objective=objective,
                    machine_watts=int(machine_watts)
                )

                result = run_util_pipeline(
                    workload_input=workload,
                    mapping_path=ZIP_PATH,
                    carbon_path=CARBON_PATH,
                    price_path=PRICE_PATH,
                    forecast_mode=forecast_mode,
                    schedule_mode=schedule_mode,
                    carbon_estimation_mode=carbon_estimation_mode,
                    historical_days=int(historical_days),
                )

                st.session_state["result"] = result
                st.session_state["last_forecast_mode_label"] = forecast_mode_label
                st.session_state["last_schedule_mode_label"] = schedule_mode_label

            except Exception as e:
                error_message = str(e)

                if forecast_mode == "live_carbon" and "WattTime" in error_message:
                    st.error(
                        "Live carbon is currently unavailable because WattTime authentication "
                        "or API access failed. Please use Demo mode or update deployment secrets/API plan."
                    )
                else:
                    st.error("An error occurred while running the pipeline.")
                    st.exception(e)

        result = st.session_state["result"]

        if result is None:
            st.info("Enter workload inputs and click Run Optimization.")
        else:
            region = result["region"]
            schedule = result["schedule"].copy()
            metrics = result["metrics"]
            forecast = result["forecast"].copy()
            optimized = result["optimized"].copy()
            
            selected_schedule = schedule[schedule["run_flag"] == 1].copy()
            run_window = build_run_window_summary(schedule)
            comparison = build_run_now_comparison(
                optimized_df=optimized,
                machine_watts=int(result["workload_input"].machine_watts)
            )

            st.subheader("Optimization Summary")

            render_status_pills(
                forecast_mode_label=st.session_state["last_forecast_mode_label"],
                schedule_mode_label=st.session_state["last_schedule_mode_label"],
                region=region,
                forecast_df=forecast
            )

            st.markdown(
                f"""
                <div class="util-card">
                    <strong>Mapped Region:</strong> {region}<br>
                    <strong>Selected Objective:</strong> {result["workload_input"].objective.title()}<br>
                    <strong>Machine Wattage:</strong> {int(result["workload_input"].machine_watts):,} W<br>
                    <strong>Recommended Window Start:</strong> {run_window["start"]}<br>
                    <strong>Recommended Window End:</strong> {run_window["end"]}<br>
                    <strong>Selected Intervals:</strong> {run_window["intervals"]}
                </div>
                """,
                unsafe_allow_html=True
            )

            k1, k2, k3, k4 = st.columns(4)
            with k1:
                render_metric_card(
                    "Optimized Carbon",
                    f"{metrics['optimized_carbon_kg']:.2f} kg"
                )
            with k2:
                render_metric_card(
                    "Carbon Reduction vs Baseline",
                    f"{metrics['carbon_reduction_pct']:.1f}%",
                    f"Saved: {metrics['carbon_savings_kg']:.2f} kg"
                )
            with k3:
                render_metric_card(
                    "Optimized Cost",
                    f"${metrics['optimized_cost']:.2f}"
                )
            with k4:
                render_metric_card(
                    "Cost Savings vs Baseline",
                    f"${metrics['cost_savings']:.2f}",
                    f"{metrics['cost_reduction_pct']:.1f}% lower"
                )

            st.subheader("Compared with Running Immediately")

            rn1, rn2 = st.columns(2)
            with rn1:
                render_metric_card(
                    "Carbon Saved vs Run Now",
                    f"{comparison['carbon_saved_vs_now_kg']:.2f} kg",
                    f"Run now: {comparison['run_now_carbon_kg']:.2f} kg"
                )
            with rn2:
                render_metric_card(
                    "Cost Saved vs Run Now",
                    f"${comparison['cost_saved_vs_now']:.2f}",
                    f"Run now: ${comparison['run_now_cost']:.2f}"
                )

            st.subheader("Recommended Schedule")

            if selected_schedule.empty:
                st.warning("No run intervals were selected.")
            else:
                st.dataframe(build_selected_schedule_df(schedule), use_container_width=True)

# ====================================================
# TAB 2 — SAVINGS ANALYSIS
# ====================================================

with tab2:
    st.header("Savings Analysis")

    result = st.session_state["result"]

    if result is None:
        st.info("Run the optimizer to view savings analysis.")
    else:
        metrics = result["metrics"]
        workload = result["workload_input"]
        optimized = result["optimized"]
        comparison = build_run_now_comparison(
            optimized_df=optimized,
            machine_watts=int(workload.machine_watts)
        )

        total_energy_kwh = comparison["optimized_df"].shape[0] * (workload.machine_watts / 1000) * (infer_interval_minutes(result["forecast"]) / 60)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            render_metric_card("Workload Energy", f"{total_energy_kwh:.2f} kWh")
        with c2:
            render_metric_card(
                "Cost Savings",
                f"${metrics['cost_savings']:.2f}",
                f"{metrics['cost_reduction_pct']:.1f}% lower than baseline"
            )
        with c3:
            render_metric_card(
                "Carbon Savings",
                f"{metrics['carbon_savings_kg']:.2f} kg",
                f"{metrics['carbon_reduction_pct']:.1f}% lower than baseline"
            )
        with c4:
            render_metric_card(
                "Saved vs Run Now",
                f"{comparison['carbon_saved_vs_now_kg']:.2f} kg CO₂",
                f"${comparison['cost_saved_vs_now']:.2f} lower cost"
            )

        st.subheader("Baseline vs Optimized")

        comparison_df = pd.DataFrame({
            "Metric": ["Cost", "Carbon"],
            "Baseline": [
                metrics["baseline_cost"],
                metrics["baseline_carbon_kg"]
            ],
            "Optimized": [
                metrics["optimized_cost"],
                metrics["optimized_carbon_kg"]
            ]
        })

        st.bar_chart(comparison_df.set_index("Metric"), use_container_width=True)

        st.subheader("Interpretation")

        st.markdown(
            f"""
            <div class="util-card">
                This run uses approximately <strong>{total_energy_kwh:.2f} kWh</strong> of electricity.
                The current recommendation reduces cost by <strong>${metrics['cost_savings']:.2f}</strong>
                and reduces emissions by <strong>{metrics['carbon_savings_kg']:.2f} kg CO₂</strong>
                versus a naive baseline schedule.
                <br><br>
                Compared with starting immediately, the optimized schedule saves
                <strong>{comparison['carbon_saved_vs_now_kg']:.2f} kg CO₂</strong>
                and <strong>${comparison['cost_saved_vs_now']:.2f}</strong>.
            </div>
            """,
            unsafe_allow_html=True
        )

# ====================================================
# TAB 3 — FORECAST SIGNALS
# ====================================================

with tab3:
    st.header("Forecast Signals")

    result = st.session_state["result"]

    if result is None:
        st.info("Run the optimizer to view forecast signals.")
    else:
        forecast = result["forecast"]
        optimized = result["optimized"]

        display_df = build_forecast_display_df(forecast, optimized)

        render_status_pills(
            forecast_mode_label=st.session_state["last_forecast_mode_label"],
            schedule_mode_label=st.session_state["last_schedule_mode_label"],
            region=result["region"],
            forecast_df=forecast
        )

def build_location_display_info(result: dict) -> dict:
    location_info = result.get("location_info", {}) or {}
    forecast_df = result.get("forecast", pd.DataFrame()).copy()

    requested_region = location_info.get("watttime_region")
    requested_region_full_name = location_info.get("watttime_region_full_name")
    latitude = location_info.get("latitude")
    longitude = location_info.get("longitude")

    forecast_region_used = None
    forecast_access_mode = None

    if not forecast_df.empty:
        if "forecast_region_used" in forecast_df.columns:
            non_null_used = forecast_df["forecast_region_used"].dropna()
            if not non_null_used.empty:
                forecast_region_used = non_null_used.iloc[0]

        if "forecast_access_mode" in forecast_df.columns:
            non_null_mode = forecast_df["forecast_access_mode"].dropna()
            if not non_null_mode.empty:
                forecast_access_mode = non_null_mode.iloc[0]

    return {
        "requested_region": requested_region,
        "requested_region_full_name": requested_region_full_name,
        "forecast_region_used": forecast_region_used,
        "forecast_access_mode": forecast_access_mode,
        "latitude": latitude,
        "longitude": longitude,
    }


def render_location_access_card(result: dict):
    info = build_location_display_info(result)

    requested_region = info["requested_region"] or result.get("region", "N/A")
    requested_region_full_name = info["requested_region_full_name"]
    forecast_region_used = info["forecast_region_used"]
    forecast_access_mode = info["forecast_access_mode"]
    latitude = info["latitude"]
    longitude = info["longitude"]

    coord_text = ""
    if latitude is not None and longitude is not None:
        coord_text = f"<br><strong>Resolved Coordinates:</strong> {latitude:.4f}, {longitude:.4f}"

    fallback_note = ""
    if forecast_access_mode == "preview_fallback":
        fallback_note = (
            "<br><br>"
            "<span class='util-warning-pill'>Preview Fallback Active</span>"
            "<br>"
            "Your ZIP code was mapped to a real WattTime region, but the current API plan "
            "does not allow live forecast access for that region. Util is using "
            "<strong>CAISO_NORTH</strong> preview forecast data so the app still works."
        )

    elif forecast_access_mode == "direct_region":
        fallback_note = (
            "<br><br>"
            "<span class='util-good-pill'>Direct Region Forecast Active</span>"
        )

    forecast_region_used_text = ""
    if forecast_region_used:
        forecast_region_used_text = (
            f"<br><strong>Forecast Region Used:</strong> {forecast_region_used}"
        )

    full_name_text = ""
    if requested_region_full_name:
        full_name_text = f"<br><strong>Resolved Region Name:</strong> {requested_region_full_name}"

    st.markdown(
        f"""
        <div class="util-card">
            <strong>Resolved Grid Region:</strong> {requested_region}
            {full_name_text}
            {forecast_region_used_text}
            {coord_text}
            {fallback_note}
        </div>
        """,
        unsafe_allow_html=True
    )


    def render_recommendation_card(result: dict, schedule_df: pd.DataFrame):
        workload = result["workload_input"]
        run_window = build_run_window_summary(schedule_df)

        objective_label = "carbon emissions" if workload.objective == "carbon" else "electricity cost"

        st.markdown(
            f"""
            <div class="util-card">
                <strong>Recommendation:</strong> Run your workload from
                <strong>{run_window["start"]}</strong> to <strong>{run_window["end"]}</strong>
                to minimize <strong>{objective_label}</strong>.
                <br><br>
                <strong>Selected Intervals:</strong> {run_window["intervals"]}<br>
                <strong>Machine Wattage:</strong> {int(workload.machine_watts):,} W<br>
                <strong>Compute Hours Required:</strong> {int(workload.compute_hours_required)} hours
            </div>
            """,
            unsafe_allow_html=True
        )

        st.subheader("Carbon Forecast with Recommended Intervals")
        st.altair_chart(build_carbon_chart(display_df), use_container_width=True)

        st.subheader("Forecast Table")

        forecast_table = display_df[[
            "hour_label",
            "carbon_g_per_kwh",
            "price_per_kwh",
            "recommended_action"
        ]].rename(columns={
            "hour_label": "Time",
            "carbon_g_per_kwh": "Carbon (g/kWh)",
            "price_per_kwh": "Price ($/kWh)",
            "recommended_action": "Recommended Action"
        })

        st.dataframe(forecast_table, use_container_width=True)

        selected_rows = display_df[display_df["run_flag"] == 1][[
            "hour_label", "carbon_g_per_kwh", "price_per_kwh"
        ]].rename(columns={
            "hour_label": "Selected Run Time",
            "carbon_g_per_kwh": "Carbon (g/kWh)",
            "price_per_kwh": "Price ($/kWh)"
        })

        st.subheader("Selected Intervals")
        st.dataframe(selected_rows, use_container_width=True)

        st.subheader("Electricity Price Forecast")
        st.line_chart(
            display_df.set_index("timestamp")[["price_per_kwh"]],
            use_container_width=True
        )

# ====================================================
# TAB 4 — RUN TIMELINE
# ====================================================

with tab4:
    st.header("Run Timeline")

    result = st.session_state["result"]

    if result is None:
        st.info("Run the optimizer to view timeline.")
    else:
        schedule = result["schedule"]
        timeline_df = build_timeline_df(schedule)

        st.markdown(
            f"""
            <div class="util-card">
                <strong>Selected run intervals:</strong><br><br>
                {build_run_hours_summary(schedule)}
            </div>
            """,
            unsafe_allow_html=True
        )

        st.dataframe(timeline_df, use_container_width=True)

# ====================================================
# TAB 5 — POWER ESTIMATOR
# ====================================================

with tab5:
    st.header("System Power Estimator")

    st.markdown(
        """
        <div class="util-card">
            Estimate approximate system power draw under compute load.
            This is useful when you do not know your machine wattage directly.
        </div>
        """,
        unsafe_allow_html=True
    )

    gpu_models = {
        "RTX 3060": 170,
        "RTX 3070": 220,
        "RTX 3080": 320,
        "RTX 3090": 350,
        "RTX 4070": 200,
        "RTX 4080": 320,
        "RTX 4090": 450,
        "A100": 400,
        "H100": 700,
        "B200": 1000
    }

    intel_cpu_options = {
        "Intel i5 / equivalent": 95,
        "Intel i7 / equivalent": 125,
        "Intel i9 / equivalent": 180,
        "Intel Xeon (single socket)": 250,
        "Intel Xeon (dual socket)": 400
    }

    amd_cpu_options = {
        "AMD Ryzen 5 / equivalent": 90,
        "AMD Ryzen 7 / equivalent": 125,
        "AMD Ryzen 9 / equivalent": 170,
        "AMD Threadripper": 280,
        "AMD EPYC (single socket)": 280,
        "AMD EPYC (dual socket)": 450
    }

    left, right = st.columns(2, gap="large")

    with left:
        gpu = st.selectbox("GPU Model", list(gpu_models.keys()))

        num_gpus = st.number_input(
            "Number of GPUs",
            min_value=1,
            max_value=100000,
            value=1,
            step=1
        )

        cpu_brand = st.selectbox("CPU Brand", ["Intel", "AMD"])

        if cpu_brand == "Intel":
            cpu_model = st.selectbox("CPU Type", list(intel_cpu_options.keys()))
            cpu_watts = intel_cpu_options[cpu_model]
        else:
            cpu_model = st.selectbox("CPU Type", list(amd_cpu_options.keys()))
            cpu_watts = amd_cpu_options[cpu_model]

    with right:
        overhead = st.slider(
            "System Overhead (RAM, motherboard, storage, cooling)",
            min_value=50,
            max_value=5000,
            value=150,
            step=10
        )

        utilization_factor = st.slider(
            "Estimated Workload Intensity",
            min_value=0.10,
            max_value=1.00,
            value=1.00,
            step=0.05
        )

    gpu_total_nameplate = gpu_models[gpu] * num_gpus
    gpu_total_estimated = gpu_total_nameplate * utilization_factor
    estimated_power = int(gpu_total_estimated + cpu_watts + overhead)
    estimated_kwh_per_hour = estimated_power / 1000

    p1, p2, p3 = st.columns(3)
    with p1:
        render_metric_card("Estimated Load", f"{estimated_power:,} W")
    with p2:
        render_metric_card("Energy Per Hour", f"{estimated_kwh_per_hour:.2f} kWh")
    with p3:
        render_metric_card("GPU Power Component", f"{int(gpu_total_estimated):,} W")

    if st.button("Use This Estimate in Optimizer"):
        st.session_state["estimated_power_watts"] = estimated_power
        st.success(
            f"Saved {estimated_power:,} W. Go back to the Optimizer tab and it is now loaded."
        )

    breakdown_df = pd.DataFrame({
        "Component": [
            "GPU model",
            "GPU count",
            "CPU brand",
            "CPU type",
            "CPU watts",
            "Overhead watts",
            "Workload intensity",
            "Estimated total watts"
        ],
        "Value": [
            gpu,
            f"{num_gpus:,}",
            cpu_brand,
            cpu_model,
            f"{cpu_watts:,} W",
            f"{overhead:,} W",
            f"{utilization_factor:.2f}",
            f"{estimated_power:,} W"
        ]
    })

    st.subheader("Estimator Breakdown")
    st.dataframe(breakdown_df, use_container_width=True)

# ====================================================
# TAB 6 — ABOUT
# ====================================================

with tab6:
    st.header("About Util")

    st.markdown(
        """
        <div class="util-card">
            <strong>Util</strong> is a compute scheduling and optimization product designed to help users run
            workloads at the best possible times to minimize electricity cost or carbon emissions.
            <br><br>
            The current MVP is recommendation-only. It does not yet automatically control workloads.
            Instead, it shows users:
            <ul>
                <li>when to run</li>
                <li>how much they can save</li>
                <li>what forecast signals drive the recommendation</li>
                <li>how much power their system is likely using</li>
            </ul>
            Future versions can add:
            <ul>
                <li>live carbon APIs ✅</li>
                <li>electricity pricing APIs</li>
                <li>system auto-detection</li>
                <li>live telemetry</li>
                <li>automated workload control</li>
                <li>multi-region scheduling</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True
    )