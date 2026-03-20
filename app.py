import base64
import streamlit as st
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import altair as alt
from PIL import Image

from src.inputs import WorkloadInput
from src.analysis.multi_location import run_multi_location_analysis
from src.pipeline import run_util_pipeline
from src.scheduling_window import InfeasibleScheduleError, INFEASIBLE_WORKLOAD_MESSAGE

# ---------------------------------------------------
# Paths
# ---------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"

LOGO_PATH = PROJECT_ROOT / "assets" / "logo" / "util_logo.png"
logo = Image.open(LOGO_PATH)
logo_base64 = base64.b64encode(LOGO_PATH.read_bytes()).decode("utf-8")

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
    @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Space+Grotesk:wght@500;700&display=swap');

    :root {
        --util-bg: #17181c;
        --util-bg-accent: #23252b;
        --util-surface: rgba(35, 39, 47, 0.84);
        --util-surface-strong: rgba(43, 47, 56, 0.94);
        --util-border: rgba(166, 176, 255, 0.14);
        --util-border-strong: rgba(168, 132, 255, 0.34);
        --util-text: #f2f3f5;
        --util-muted: #b5bac1;
        --util-accent: #8b5cf6;
        --util-accent-strong: #6d28d9;
        --util-good: #4ade80;
        --util-warn: #8b5cf6;
        --util-shadow: 0 24px 70px rgba(0, 0, 0, 0.34);
    }

    .stApp {
        background:
            radial-gradient(circle at top left, rgba(139, 92, 246, 0.18), transparent 28%),
            radial-gradient(circle at top right, rgba(88, 101, 242, 0.1), transparent 24%),
            linear-gradient(180deg, var(--util-bg) 0%, #101114 100%);
        color: var(--util-text);
        font-family: 'Manrope', sans-serif;
    }

    .block-container {
        padding-top: 1.35rem;
        padding-bottom: 2.5rem;
        max-width: 1320px;
    }

    html, body, [class*="css"], [data-testid="stAppViewContainer"] {
        font-family: 'Manrope', sans-serif;
    }

    h1, h2, h3, .util-brand-title {
        color: var(--util-text);
        font-family: 'Space Grotesk', sans-serif;
        letter-spacing: -0.03em;
    }

    h1 {
        font-size: clamp(2.2rem, 4vw, 3.8rem);
        line-height: 0.95;
        margin-bottom: 0.4rem;
    }

    h2 {
        font-size: 1.35rem;
        margin-bottom: 0.8rem;
    }

    h3 {
        font-size: 1.02rem;
    }

    .util-subtext {
        color: var(--util-muted);
        font-size: 1.02rem;
        line-height: 1.7;
        margin-bottom: 0;
    }

    .util-hero {
        position: relative;
        overflow: hidden;
        background:
            linear-gradient(135deg, rgba(43, 47, 56, 0.94), rgba(28, 30, 34, 0.96)),
            radial-gradient(circle at 20% 20%, rgba(139, 92, 246, 0.16), transparent 30%);
        border: 1px solid var(--util-border);
        border-radius: 28px;
        padding: 1.55rem 1.6rem;
        box-shadow: var(--util-shadow);
        margin-bottom: 1.25rem;
    }

    .util-hero::after {
        content: "";
        position: absolute;
        inset: auto -8% -35% auto;
        width: 260px;
        height: 260px;
        border-radius: 50%;
        background: radial-gradient(circle, rgba(139, 92, 246, 0.22), transparent 70%);
        pointer-events: none;
    }

    .util-hero-grid {
        display: grid;
        grid-template-columns: minmax(0, 1fr);
        gap: 1.1rem;
        align-items: start;
    }

    .util-brand-row {
        display: flex;
        align-items: center;
        gap: 0.95rem;
        margin-bottom: 0.75rem;
    }

    .util-logo-shell {
        width: 72px;
        height: 72px;
        border-radius: 20px;
        background: linear-gradient(180deg, rgba(139, 92, 246, 0.18), rgba(109, 40, 217, 0.08));
        border: 1px solid rgba(168, 132, 255, 0.24);
        display: flex;
        align-items: center;
        justify-content: center;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
    }

    .util-brand-kicker {
        color: #c9b8ff;
        text-transform: uppercase;
        letter-spacing: 0.18em;
        font-size: 0.75rem;
        margin-bottom: 0.35rem;
    }

    .util-header-pills {
        display: flex;
        flex-wrap: wrap;
        gap: 0.6rem;
        margin-top: 1rem;
    }

    .util-header-pill {
        padding: 0.55rem 0.8rem;
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.04);
        border: 1px solid rgba(168, 132, 255, 0.18);
        color: #e4defc;
        font-size: 0.88rem;
    }

    .util-card {
        background: var(--util-surface);
        border: 1px solid var(--util-border);
        border-radius: 22px;
        padding: 1.15rem 1.2rem;
        box-shadow: var(--util-shadow);
        backdrop-filter: blur(14px);
        margin-bottom: 1rem;
    }

    .util-metric-label {
        color: var(--util-muted);
        font-size: 0.83rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        margin-bottom: 0.45rem;
    }

    .util-metric-value {
        color: var(--util-text);
        font-size: 2rem;
        font-family: 'Space Grotesk', sans-serif;
        font-weight: 700;
        line-height: 1.1;
    }

    .util-metric-delta {
        color: #c4b2ff;
        font-size: 0.92rem;
        margin-top: 0.5rem;
    }

    div[data-baseweb="tab-list"] {
        gap: 0.65rem;
        background: rgba(32, 34, 37, 0.72);
        border: 1px solid rgba(168, 132, 255, 0.1);
        padding: 0.45rem;
        border-radius: 20px;
        backdrop-filter: blur(14px);
    }

    button[data-baseweb="tab"] {
        background: transparent !important;
        color: #b8bcc5 !important;
        border-radius: 14px;
        padding: 0.68rem 1.05rem;
        border: 1px solid transparent;
        font-size: 0.96rem;
        transition: all 0.2s ease;
    }

    button[data-baseweb="tab"][aria-selected="true"] {
        background: linear-gradient(180deg, rgba(139, 92, 246, 0.22), rgba(109, 40, 217, 0.28)) !important;
        color: #f4f9ff !important;
        border: 1px solid var(--util-border-strong) !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
    }

    div.stButton > button {
        background: linear-gradient(135deg, var(--util-accent) 0%, var(--util-accent-strong) 100%);
        color: white !important;
        border: 1px solid rgba(168, 132, 255, 0.22);
        border-radius: 14px;
        font-weight: 600;
        padding: 0.7rem 1rem;
        min-height: 2.85rem;
        box-shadow: 0 16px 32px rgba(69, 39, 120, 0.3);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    div.stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 18px 34px rgba(69, 39, 120, 0.36);
    }

    div.stDownloadButton > button {
        background: rgba(255, 255, 255, 0.03);
        color: #ddd6fe !important;
        border: 1px solid rgba(139, 92, 246, 0.22);
        border-radius: 14px;
        font-weight: 600;
        padding: 0.68rem 0.95rem;
        min-height: 2.75rem;
        box-shadow: none;
        backdrop-filter: blur(10px);
        transition: transform 0.2s ease, border-color 0.2s ease, background 0.2s ease;
    }

    div.stDownloadButton > button:hover {
        transform: translateY(-1px);
        background: rgba(139, 92, 246, 0.1);
        border-color: rgba(139, 92, 246, 0.36);
        box-shadow: none;
    }

    div.stButton,
    div.stDownloadButton {
        margin-top: 0.3rem;
        margin-bottom: 0.35rem;
    }

    div[data-testid="stMetric"],
    div[data-testid="stDataFrame"],
    div[data-testid="stAlert"],
    div[data-testid="stMarkdownContainer"] > div.util-card {
        border-radius: 20px;
    }

    div[data-testid="stMetric"] {
        background: var(--util-surface-strong);
        border: 1px solid var(--util-border);
        padding: 1rem;
        box-shadow: var(--util-shadow);
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid var(--util-border);
        background: var(--util-surface-strong);
        border-radius: 20px;
        overflow: hidden;
        box-shadow: var(--util-shadow);
    }

    div[data-testid="stAlert"] {
        background: rgba(139, 92, 246, 0.1);
        border: 1px solid rgba(139, 92, 246, 0.2);
        color: var(--util-text);
        box-shadow: var(--util-shadow);
    }

    div[data-testid="stAlert"] svg {
        fill: #8b5cf6;
        color: #8b5cf6;
    }

    .util-pill {
        display: inline-block;
        padding: 0.45rem 0.72rem;
        border-radius: 999px;
        background: rgba(139, 92, 246, 0.12);
        color: #ddd6fe;
        font-size: 0.82rem;
        border: 1px solid rgba(139, 92, 246, 0.18);
        margin-right: 0.4rem;
        margin-bottom: 0.4rem;
    }

    .util-good-pill {
        display: inline-block;
        padding: 0.35rem 0.7rem;
        border-radius: 999px;
        background: rgba(61, 213, 152, 0.14);
        color: #c8ffe8;
        font-size: 0.82rem;
        border: 1px solid rgba(61, 213, 152, 0.22);
        margin-right: 0.4rem;
        margin-bottom: 0.4rem;
    }

    .util-warning-pill {
        display: inline-block;
        padding: 0.35rem 0.7rem;
        border-radius: 999px;
        background: rgba(139, 92, 246, 0.12);
        color: #ddd6fe;
        font-size: 0.82rem;
        border: 1px solid rgba(139, 92, 246, 0.22);
        margin-right: 0.4rem;
        margin-bottom: 0.4rem;
    }

    label, .stMarkdown, .stCaption, .stTextInput label, .stNumberInput label {
        color: var(--util-text) !important;
    }

    [data-testid="stToolbar"] {
        visibility: hidden;
        height: 0;
        position: fixed;
    }

    div[data-baseweb="select"] > div,
    div[data-baseweb="input"] > div,
    .stDateInput > div > div,
    .stNumberInput > div > div,
    .stTextInput > div > div,
    .stTextArea textarea {
        background: rgba(30, 31, 34, 0.9) !important;
        border: 1px solid rgba(168, 132, 255, 0.18) !important;
        color: var(--util-text) !important;
        border-radius: 16px !important;
    }

    .stRadio [role="radiogroup"] {
        gap: 0.5rem;
        background: rgba(32, 34, 37, 0.58);
        padding: 0.35rem;
        border: 1px solid rgba(168, 132, 255, 0.12);
        border-radius: 16px;
    }

    .stSlider [data-baseweb="slider"] {
        padding-top: 0.7rem;
        padding-bottom: 0.35rem;
    }

    .stTabs {
        margin-top: 0.5rem;
    }

    @media (max-width: 900px) {
        .util-hero-grid {
            grid-template-columns: 1fr;
        }

        .util-hero {
            padding: 1.2rem;
        }
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

    line = base.mark_line(color="#a78bfa", strokeWidth=3).encode(
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


def build_metric_comparison_chart(comparison_df: pd.DataFrame) -> alt.Chart:
    chart_df = comparison_df.melt(
        id_vars="Metric",
        value_vars=["Baseline", "Optimized"],
        var_name="Scenario",
        value_name="Value"
    )

    return alt.Chart(chart_df).mark_bar(
        cornerRadiusTopLeft=8,
        cornerRadiusTopRight=8
    ).encode(
        x=alt.X("Metric:N", title=None, axis=alt.Axis(labelColor="#b5bac1")),
        xOffset="Scenario:N",
        y=alt.Y(
            "Value:Q",
            title=None,
            axis=alt.Axis(labelColor="#b5bac1", gridColor="rgba(255,255,255,0.08)")
        ),
        color=alt.Color(
            "Scenario:N",
            scale=alt.Scale(
                domain=["Baseline", "Optimized"],
                range=["#6b7280", "#8b5cf6"]
            ),
            legend=alt.Legend(title=None, labelColor="#d1d5db")
        ),
        tooltip=[
            alt.Tooltip("Metric:N"),
            alt.Tooltip("Scenario:N"),
            alt.Tooltip("Value:Q", format=".3f")
        ]
    ).properties(height=320)


def build_price_chart(display_df: pd.DataFrame) -> alt.Chart:
    chart_df = display_df.copy()
    chart_df["timestamp"] = pd.to_datetime(chart_df["timestamp"])

    return alt.Chart(chart_df).mark_line(color="#8b5cf6", strokeWidth=3).encode(
        x=alt.X("timestamp:T", title="Time"),
        y=alt.Y("price_per_kwh:Q", title="Price ($/kWh)"),
        tooltip=[
            alt.Tooltip("timestamp:T", title="Time"),
            alt.Tooltip("price_per_kwh:Q", title="Price ($/kWh)", format=".3f")
        ]
    ).properties(height=300)


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


def render_recommendation_card(
    result: dict,
    schedule_df: pd.DataFrame,
    display_df: pd.DataFrame
):
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
    st.altair_chart(build_price_chart(display_df), use_container_width=True)


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

st.markdown(
    """
    <div class="util-hero">
        <div class="util-hero-grid">
            <div>
                <div class="util-brand-row">
                    <div class="util-logo-shell">
                        <img src="data:image/png;base64,{}" width="42" />
                    </div>
                    <div>
                        <div class="util-brand-kicker">Intelligent Compute Scheduling</div>
                        <h1 class="util-brand-title">Util</h1>
                    </div>
                </div>
                <div class="util-subtext">
                    Schedule compute workloads at cleaner and cheaper times without changing your workflow.
                    The current experience is designed like a product dashboard so the same structure can carry cleanly into a future native app.
                </div>
                <div class="util-header-pills">
                    <span class="util-header-pill">Cost-aware optimization</span>
                    <span class="util-header-pill">Carbon-aware scheduling</span>
                    <span class="util-header-pill">Forecast-driven recommendations</span>
                </div>
            </div>
        </div>
    </div>
    """
    .format(logo_base64),
    unsafe_allow_html=True
)

# ---------------------------------------------------
# Tabs
# ---------------------------------------------------

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "Optimizer",
    "Savings Analysis",
    "Forecast Signals",
    "Run Timeline",
    "Power Estimator",
    "Multi-Location",
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
            [
                "Short-Term (Live Data - 24 hour access)",
                "Extended (Estimated Forecast)",
            ],
            horizontal=True
        )
        st.caption(
            "Extended mode uses historical data to estimate beyond the live forecast window."
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
                    if carbon_estimation_mode_label == "Extended (Estimated Forecast)"
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
                elif isinstance(e, InfeasibleScheduleError):
                    st.error(INFEASIBLE_WORKLOAD_MESSAGE)
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

        st.altair_chart(build_metric_comparison_chart(comparison_df), use_container_width=True)

        savings_export_df = pd.DataFrame([
            {
                "zip_code": workload.zip_code,
                "region": result["region"],
                "objective": workload.objective,
                "compute_hours_required": workload.compute_hours_required,
                "deadline": workload.deadline,
                "machine_watts": workload.machine_watts,
                "optimized_cost": metrics["optimized_cost"],
                "baseline_cost": metrics["baseline_cost"],
                "cost_savings": metrics["cost_savings"],
                "cost_reduction_pct": metrics["cost_reduction_pct"],
                "optimized_carbon_kg": metrics["optimized_carbon_kg"],
                "baseline_carbon_kg": metrics["baseline_carbon_kg"],
                "carbon_savings_kg": metrics["carbon_savings_kg"],
                "carbon_reduction_pct": metrics["carbon_reduction_pct"],
            }
        ])
        savings_csv = savings_export_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download Savings CSV",
            data=savings_csv,
            file_name="util_savings_analysis.csv",
            mime="text/csv",
        )

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

        render_location_access_card(result)
        render_recommendation_card(result, result["schedule"], display_df)

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
# TAB 6 — MULTI-LOCATION
# ====================================================

with tab6:
    st.header("Multi-Location")

    st.markdown(
        '<div class="util-card">Compare the same workload across multiple ZIP codes.</div>',
        unsafe_allow_html=True
    )

    multi_location_input = st.text_input(
        "Enter ZIP codes (comma separated)",
        "93106, 10001, 60601"
    )
    zip_codes = [z.strip() for z in multi_location_input.split(",") if z.strip()]

    compare_locations_button = st.button("Compare Locations")

    if compare_locations_button:
        if not zip_codes:
            st.warning("Enter at least one ZIP code to compare.")
        else:
            try:
                forecast_mode = "live_carbon" if forecast_mode_label == "Live Carbon" else "demo"
                schedule_mode = "block" if schedule_mode_label == "Continuous Block" else "flexible"

                multi_location_results = run_multi_location_analysis(
                    zip_codes=zip_codes,
                    compute_hours_required=int(compute_hours),
                    deadline=deadline,
                    objective=objective,
                    machine_watts=int(machine_watts),
                    mapping_path=ZIP_PATH,
                    forecast_mode=forecast_mode,
                    schedule_mode=schedule_mode,
                )

                if multi_location_results.empty:
                    st.info("No location results were returned.")
                else:
                    st.dataframe(multi_location_results, use_container_width=True)
                    multi_location_csv = multi_location_results.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "Download CSV",
                        data=multi_location_csv,
                        file_name="util_multi_location_results.csv",
                        mime="text/csv",
                    )

                    lowest_cost_row = multi_location_results.loc[
                        multi_location_results["optimized_cost"].idxmin()
                    ]
                    lowest_carbon_row = multi_location_results.loc[
                        multi_location_results["optimized_carbon_kg"].idxmin()
                    ]

                    st.write(f"Lowest Cost Location: ZIP {lowest_cost_row['zip_code']}")
                    st.write(f"Lowest Carbon Location: ZIP {lowest_carbon_row['zip_code']}")

            except Exception as e:
                error_message = str(e)

                if forecast_mode == "live_carbon" and "WattTime" in error_message:
                    st.error(
                        "Live carbon is currently unavailable because WattTime authentication "
                        "or API access failed. Please use Demo mode or update deployment secrets/API plan."
                    )
                elif isinstance(e, InfeasibleScheduleError):
                    st.error(INFEASIBLE_WORKLOAD_MESSAGE)
                else:
                    st.error("An error occurred while comparing locations.")
                    st.exception(e)

# ====================================================
# TAB 7 — ABOUT
# ====================================================

with tab7:
    st.header("About Util")

    st.markdown(
        """
        <div class="util-card">
            <strong>Util</strong> is a compute scheduling and optimization product designed to help users run
            workloads at the best possible times and locations in order to minimize electricity costs and carbon emissions.
            <br><br>
            The current MVP is recommendation-only. It does not yet automatically control workloads or locations.
            Instead, it shows users:
            <ul>
                <li>when to run</li>
                <li>how much they can save</li>
                <li>what forecast signals drive the recommendation</li>
                <li>how much power their system is likely using</li>
            </ul>
            Future versions can add:
            <ul>
                <li>live carbon APIs (complete)</li>
                <li>electricity pricing APIs</li>
                <li>system auto-detection</li>
                <li>live telemetry</li>
                <li>automated workload control</li>
                <li>multi-region scheduling</li>
                <li>work with electricity providers to solve the issue from a supply side</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True
    )
