import streamlit as st
from datetime import datetime
from pathlib import Path
import pandas as pd
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


def build_forecast_display_df(forecast_df: pd.DataFrame, schedule_df: pd.DataFrame | None = None) -> pd.DataFrame:
    df = forecast_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour_label"] = df["timestamp"].dt.strftime("%b %d, %I:%M %p")

    if schedule_df is not None:
        run_lookup = schedule_df[["timestamp", "run_flag", "recommended_action"]].copy()
        run_lookup["timestamp"] = pd.to_datetime(run_lookup["timestamp"])
        df = df.merge(run_lookup, on="timestamp", how="left")
    else:
        df["run_flag"] = 0
        df["recommended_action"] = "Unknown"

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


def build_run_hours_summary(schedule_df: pd.DataFrame) -> str:
    run_rows = schedule_df[schedule_df["run_flag"] == 1].copy()
    if run_rows.empty:
        return "No run hours selected."

    run_rows["timestamp"] = pd.to_datetime(run_rows["timestamp"])
    labels = run_rows["timestamp"].dt.strftime("%b %d %I:%M %p").tolist()
    return " • ".join(labels)


# ---------------------------------------------------
# Session State Defaults
# ---------------------------------------------------

if "estimated_power_watts" not in st.session_state:
    st.session_state["estimated_power_watts"] = 300

if "result" not in st.session_state:
    st.session_state["result"] = None

# ---------------------------------------------------
# Header
# ---------------------------------------------------

col_logo, col_title = st.columns([1,6])

with col_logo:
    st.image(logo, width=80)

with col_title:
    st.markdown(
        """
        # Util
        """
    )

st.markdown(
"""
Compute scheduling software that minimizes **electricity cost** or **carbon emissions** by choosing the best hours to run workloads.
"""
)

st.markdown(
    """
    <div class="util-subtext">
    Compute scheduling and optimization software for minimizing electricity cost or carbon emissions.
    </div>
    """,
    unsafe_allow_html=True
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

        machine_watts = st.number_input(
            "Machine Wattage (Watts)",
            min_value=50,
            max_value=500000,
            value=int(st.session_state["estimated_power_watts"]),
            step=10
        )

        deadline = st.datetime_input(
            "Deadline",
            value=datetime(2026, 3, 13, 17, 0)
        )

        run_button = st.button("Run Optimization")

        st.markdown("</div>", unsafe_allow_html=True)

        st.caption(
            "Tip: use the Power Estimator tab to generate a machine wattage estimate."
        )

    with col_output:
        if run_button:
            try:
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
                    price_path=PRICE_PATH
                )

                st.session_state["result"] = result

            except Exception as e:
                st.error("An error occurred while running the pipeline.")
                st.exception(e)

        result = st.session_state["result"]

        if result is None:
            st.info("Enter workload inputs and click Run Optimization.")
        else:
            region = result["region"]
            schedule = result["schedule"].copy()
            metrics = result["metrics"]

            st.subheader("Optimization Summary")
            st.markdown(
                f"""
                <div class="util-card">
                    <strong>Mapped Region:</strong> {region}<br>
                    <strong>Selected Objective:</strong> {objective.title()}<br>
                    <strong>Machine Wattage:</strong> {int(machine_watts):,} W
                </div>
                """,
                unsafe_allow_html=True
            )

            m1, m2, m3, m4 = st.columns(4)
            with m1:
                render_metric_card("Baseline Cost", f"${metrics['baseline_cost']:.2f}")
            with m2:
                render_metric_card(
                    "Optimized Cost",
                    f"${metrics['optimized_cost']:.2f}",
                    f"Savings: ${metrics['cost_savings']:.2f}"
                )
            with m3:
                render_metric_card(
                    "Baseline Carbon",
                    f"{metrics['baseline_carbon_kg']:.2f} kg"
                )
            with m4:
                render_metric_card(
                    "Optimized Carbon",
                    f"{metrics['optimized_carbon_kg']:.2f} kg",
                    f"Reduction: {metrics['carbon_reduction_pct']:.1f}%"
                )

            st.subheader("Recommended Schedule")

            display_schedule = schedule.copy()
            display_schedule["timestamp"] = pd.to_datetime(display_schedule["timestamp"])
            display_schedule["timestamp"] = display_schedule["timestamp"].dt.strftime("%b %d, %I:%M %p")

            st.dataframe(display_schedule, use_container_width=True)

# ====================================================
# TAB 2 — SAVINGS ANALYSIS
# ====================================================

with tab2:
    st.header("Savings Analysis")

    result = st.session_state["result"]

    if result is None:
        st.info("Run the optimizer first.")
    else:
        metrics = result["metrics"]
        workload = result["workload_input"]
        total_energy_kwh = (workload.machine_watts / 1000) * workload.compute_hours_required

        c1, c2, c3 = st.columns(3)
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
                Under the placeholder forecast, optimizing for <strong>{workload.objective}</strong> reduces
                cost by <strong>${metrics['cost_savings']:.2f}</strong> and reduces emissions by
                <strong>{metrics['carbon_savings_kg']:.2f} kg CO₂</strong> relative to a naive schedule.
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
        schedule = result["schedule"]

        display_df = build_forecast_display_df(forecast, schedule)

        st.subheader("Hourly Forecast Table")

        forecast_table = display_df[[
            "hour_label",
            "carbon_g_per_kwh",
            "price_per_kwh",
            "recommended_action"
        ]].rename(columns={
            "hour_label": "Hour",
            "carbon_g_per_kwh": "Carbon (g/kWh)",
            "price_per_kwh": "Price ($/kWh)",
            "recommended_action": "Recommended Action"
        })

        st.dataframe(forecast_table, use_container_width=True)

        st.subheader("Carbon Intensity Forecast")
        st.line_chart(
            display_df.set_index("timestamp")[["carbon_g_per_kwh"]],
            use_container_width=True
        )

        run_carbon = display_df[display_df["run_flag"] == 1][[
            "hour_label", "carbon_g_per_kwh"
        ]].rename(columns={
            "hour_label": "Selected Run Hour",
            "carbon_g_per_kwh": "Carbon (g/kWh)"
        })

        st.caption("Recommended run hours under current optimization:")
        st.dataframe(run_carbon, use_container_width=True)

        st.subheader("Electricity Price Forecast")
        st.line_chart(
            display_df.set_index("timestamp")[["price_per_kwh"]],
            use_container_width=True
        )

        run_price = display_df[display_df["run_flag"] == 1][[
            "hour_label", "price_per_kwh"
        ]].rename(columns={
            "hour_label": "Selected Run Hour",
            "price_per_kwh": "Price ($/kWh)"
        })

        st.caption("Price levels during selected run hours:")
        st.dataframe(run_price, use_container_width=True)

# ====================================================
# TAB 4 — RUN TIMELINE
# ====================================================

with tab4:
    st.header("Run Timeline")

    result = st.session_state["result"]

    if result is None:
        st.info("Run the optimizer first.")
    else:
        schedule = result["schedule"]
        timeline_df = build_timeline_df(schedule)

        st.markdown(
            f"""
            <div class="util-card">
                <strong>Selected run hours:</strong><br><br>
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
            f"Saved {estimated_power:,} W. Go back to the Optimizer tab and it will appear as the default machine wattage."
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
                <li>live carbon APIs</li>
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