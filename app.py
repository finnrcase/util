import base64
import os
import streamlit as st
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import altair as alt
from PIL import Image

from src.admin_dashboard import build_run_analytics_record
from src.analytics import append_run
from src.inputs import WorkloadInput
from src.metrics import add_interval_impact_columns
from src.analysis.multi_location import run_multi_location_analysis
from src.data_fetcher import build_live_historical_export_table
from src.pipeline import run_util_pipeline
from src.scheduling_window import (
    APP_TIMEZONE,
    InfeasibleScheduleError,
    INFEASIBLE_WORKLOAD_MESSAGE,
)

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
ANALYTICS_PATH = PROJECT_ROOT / "data" / "analytics" / "util_admin_analytics.csv"
ANALYTICS_LOGGING_ENABLED = os.getenv("UTIL_ANALYTICS_ENABLED", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DEFAULT_ANALYTICS_RUN_TYPE = os.getenv("UTIL_ANALYTICS_RUN_TYPE", "Real").strip() or "Real"

# ---------------------------------------------------
# Page Setup
# ---------------------------------------------------

st.set_page_config(
    page_title="Util",
    page_icon="⚡",
    layout="wide"
)

THEME_TOKENS = {
    "bg": "#090b13",
    "bg_top": "#140f26",
    "surface": "rgba(22, 16, 38, 0.76)",
    "surface_strong": "rgba(20, 14, 34, 0.9)",
    "surface_soft": "rgba(255, 255, 255, 0.045)",
    "surface_elevated": "rgba(28, 19, 45, 0.92)",
    "border": "rgba(184, 145, 255, 0.2)",
    "border_strong": "rgba(184, 145, 255, 0.38)",
    "border_inner": "rgba(255, 255, 255, 0.08)",
    "text": "#fbf8ff",
    "text_muted": "#b3a8cf",
    "text_soft": "#e2daf3",
    "accent_blue": "#7b6dff",
    "accent_blue_soft": "rgba(123, 109, 255, 0.22)",
    "accent_violet": "#a77bff",
    "accent_violet_soft": "rgba(167, 123, 255, 0.26)",
    "accent_silver": "#efe7ff",
    "success": "#6ee7b7",
    "success_soft": "rgba(110, 231, 183, 0.18)",
    "shadow": "0 28px 80px rgba(0, 0, 0, 0.42)",
    "shadow_soft": "0 18px 48px rgba(2, 6, 23, 0.28)",
    "radius_lg": "28px",
    "radius_md": "22px",
    "radius_sm": "16px",
}


def build_theme_css(tokens: dict[str, str]) -> str:
    return """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Space+Grotesk:wght@500;700&display=swap');

    :root {{
        --util-bg: {bg};
        --util-bg-top: {bg_top};
        --util-surface: {surface};
        --util-surface-strong: {surface_strong};
        --util-surface-soft: {surface_soft};
        --util-surface-elevated: {surface_elevated};
        --util-border: {border};
        --util-border-strong: {border_strong};
        --util-border-inner: {border_inner};
        --util-text: {text};
        --util-muted: {text_muted};
        --util-text-soft: {text_soft};
        --util-accent-blue: {accent_blue};
        --util-accent-blue-soft: {accent_blue_soft};
        --util-accent-violet: {accent_violet};
        --util-accent-violet-soft: {accent_violet_soft};
        --util-accent-silver: {accent_silver};
        --util-good: {success};
        --util-good-soft: {success_soft};
        --util-shadow: {shadow};
        --util-shadow-soft: {shadow_soft};
        --util-radius-lg: {radius_lg};
        --util-radius-md: {radius_md};
        --util-radius-sm: {radius_sm};
    }}

    .stApp {{
        background:
            radial-gradient(circle at top left, rgba(167, 123, 255, 0.22), transparent 28%),
            radial-gradient(circle at 78% 10%, rgba(123, 109, 255, 0.16), transparent 24%),
            radial-gradient(circle at 50% 32%, rgba(255, 255, 255, 0.035), transparent 22%),
            linear-gradient(180deg, var(--util-bg-top) 0%, var(--util-bg) 58%, #05070d 100%);
        color: var(--util-text);
        font-family: 'Manrope', sans-serif;
    }}

    .stApp::before {{
        content: "";
        position: fixed;
        inset: 0;
        pointer-events: none;
        background:
            radial-gradient(circle at 16% 22%, rgba(167, 123, 255, 0.18), transparent 20%),
            radial-gradient(circle at 82% 18%, rgba(123, 109, 255, 0.12), transparent 16%),
            radial-gradient(circle at 50% 60%, rgba(255, 255, 255, 0.04), transparent 28%);
        filter: blur(18px);
        opacity: 0.9;
    }}

    .block-container {{
        padding-top: 1.4rem;
        padding-bottom: 2.8rem;
        max-width: 1360px;
    }}

    html, body, [class*="css"], [data-testid="stAppViewContainer"] {{
        font-family: 'Manrope', sans-serif;
    }}

    h1, h2, h3, .util-brand-title, .util-section-title {{
        color: var(--util-text);
        font-family: 'Space Grotesk', sans-serif;
        letter-spacing: -0.04em;
    }}

    h1 {{
        font-size: clamp(2.65rem, 5vw, 4.65rem);
        line-height: 0.92;
        margin-bottom: 0.4rem;
    }}

    h2 {{
        font-size: 1.35rem;
        margin-bottom: 0.5rem;
    }}

    h3 {{
        font-size: 1.02rem;
        margin-bottom: 0.45rem;
    }}

    p, label, .stMarkdown, .stCaption, .stTextInput label, .stNumberInput label {{
        color: var(--util-text) !important;
    }}

    [data-testid="stToolbar"] {{
        visibility: hidden;
        height: 0;
        position: fixed;
    }}

    .util-hero {{
        position: relative;
        overflow: hidden;
        background:
            linear-gradient(140deg, rgba(25, 18, 42, 0.94), rgba(12, 9, 24, 0.86)),
            radial-gradient(circle at 18% 18%, rgba(167, 123, 255, 0.26), transparent 30%),
            radial-gradient(circle at 82% 22%, rgba(123, 109, 255, 0.12), transparent 26%);
        border: 1px solid var(--util-border);
        border-radius: 30px;
        padding: 1rem 1.25rem;
        box-shadow: var(--util-shadow);
        backdrop-filter: blur(22px);
        margin-bottom: 0.9rem;
        isolation: isolate;
    }}

    .util-hero::before {{
        content: "";
        position: absolute;
        inset: 1px;
        border-radius: inherit;
        border: 1px solid rgba(255, 255, 255, 0.06);
        pointer-events: none;
        mask: linear-gradient(180deg, rgba(255,255,255,0.8), transparent 46%);
    }}

    .util-hero::after {{
        content: "";
        position: absolute;
        inset: auto -8% -60% auto;
        width: 240px;
        height: 240px;
        border-radius: 50%;
        background: radial-gradient(circle, rgba(167, 123, 255, 0.24), transparent 70%);
        pointer-events: none;
        filter: blur(12px);
        opacity: 0.9;
    }}

    .util-hero-grid {{
        display: flex;
        align-items: center;
        justify-content: flex-start;
        gap: 0.95rem;
        position: relative;
        z-index: 1;
    }}

    .util-brand-row {{
        display: flex;
        align-items: center;
        gap: 0.95rem;
        margin-bottom: 0;
    }}

    .util-logo-shell {{
        width: 62px;
        height: 62px;
        border-radius: 18px;
        background:
            linear-gradient(180deg, rgba(255, 255, 255, 0.08), rgba(255,255,255,0.02)),
            linear-gradient(135deg, rgba(167, 123, 255, 0.24), rgba(123, 109, 255, 0.12));
        border: 1px solid rgba(184, 145, 255, 0.28);
        display: flex;
        align-items: center;
        justify-content: center;
        box-shadow:
            inset 0 1px 0 rgba(255,255,255,0.1),
            0 18px 38px rgba(11, 16, 35, 0.45);
        backdrop-filter: blur(16px);
    }}

    .util-brand-kicker,
    .util-section-kicker {{
        color: #d9c8ff;
        text-transform: uppercase;
        letter-spacing: 0.18em;
        font-size: 0.75rem;
        font-weight: 700;
        margin-bottom: 0.35rem;
    }}

    .util-brand-title {{
        margin: 0;
        font-size: clamp(2rem, 4vw, 3rem);
        line-height: 1;
    }}

    .util-header-pills,
    .util-pill-row {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.6rem;
        margin-top: 1rem;
    }}

    .util-header-pill,
    .util-pill,
    .util-good-pill,
    .util-warning-pill {{
        display: inline-flex;
        align-items: center;
        gap: 0.38rem;
        padding: 0.5rem 0.78rem;
        border-radius: 999px;
        font-size: 0.82rem;
        font-weight: 600;
        letter-spacing: 0.01em;
        backdrop-filter: blur(12px);
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
    }}

    .util-header-pill,
    .util-pill,
    .util-warning-pill {{
        background: rgba(255, 255, 255, 0.05);
        color: var(--util-text-soft);
        border: 1px solid rgba(157, 180, 255, 0.16);
    }}

    .util-good-pill {{
        background: var(--util-good-soft);
        color: #dcfff0;
        border: 1px solid rgba(110, 231, 183, 0.26);
    }}

    .util-glass-card,
    .util-card {{
        position: relative;
        overflow: hidden;
        background:
            linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.015)),
            linear-gradient(140deg, rgba(18, 23, 38, 0.86), rgba(10, 13, 22, 0.76));
        border: 1px solid var(--util-border);
        border-radius: var(--util-radius-md);
        padding: 1.2rem 1.2rem;
        box-shadow: var(--util-shadow-soft);
        backdrop-filter: blur(18px);
        margin-bottom: 1rem;
    }}

    .util-glass-card::before,
    .util-card::before {{
        content: "";
        position: absolute;
        inset: 0;
        border-radius: inherit;
        border: 1px solid rgba(255, 255, 255, 0.05);
        pointer-events: none;
        mask: linear-gradient(180deg, rgba(255,255,255,0.75), transparent 48%);
    }}

    .util-section-shell {{
        position: relative;
        overflow: hidden;
        background:
            linear-gradient(180deg, rgba(255,255,255,0.035), rgba(255,255,255,0.01)),
            linear-gradient(145deg, rgba(12, 16, 28, 0.94), rgba(9, 12, 22, 0.86));
        border: 1px solid rgba(157, 180, 255, 0.16);
        border-radius: 30px;
        padding: 1.15rem 1.15rem 1.25rem;
        margin-top: 0.35rem;
        margin-bottom: 1rem;
        box-shadow: var(--util-shadow);
        backdrop-filter: blur(20px);
    }}

    .util-section-shell::after {{
        content: "";
        position: absolute;
        inset: auto 10% -55% auto;
        width: 240px;
        height: 240px;
        border-radius: 50%;
        background: radial-gradient(circle, rgba(139, 92, 246, 0.14), transparent 72%);
        filter: blur(8px);
        pointer-events: none;
    }}

    .util-section-heading {{
        position: relative;
        z-index: 1;
        display: flex;
        justify-content: space-between;
        gap: 1rem;
        align-items: end;
        margin-bottom: 1rem;
    }}

    .util-section-copy {{
        max-width: 48rem;
    }}

    .util-section-description {{
        color: var(--util-muted);
        font-size: 0.98rem;
        line-height: 1.7;
    }}

    .util-side-note {{
        min-width: 220px;
        padding: 0.85rem 1rem;
        border-radius: 20px;
        background: linear-gradient(180deg, rgba(88, 166, 255, 0.14), rgba(255,255,255,0.02));
        border: 1px solid rgba(120, 164, 255, 0.2);
        color: var(--util-text-soft);
        font-size: 0.9rem;
        line-height: 1.55;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.06);
    }}

    .util-callout-grid {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 1rem;
    }}

    .util-callout {{
        padding: 1rem 1.05rem;
        border-radius: 22px;
        background:
            linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.015)),
            linear-gradient(140deg, rgba(14, 18, 30, 0.9), rgba(10, 12, 22, 0.76));
        border: 1px solid rgba(157, 180, 255, 0.16);
        box-shadow: var(--util-shadow-soft);
        min-height: 106px;
        margin-bottom: 0.9rem;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        gap: 0.5rem;
    }}

    .util-callout-label {{
        color: var(--util-muted);
        text-transform: uppercase;
        letter-spacing: 0.14em;
        font-size: 0.76rem;
        margin-bottom: 0.35rem;
        line-height: 1.35;
        overflow-wrap: anywhere;
    }}

    .util-callout-value {{
        color: var(--util-text);
        font-size: 1.1rem;
        font-weight: 700;
        font-family: 'Space Grotesk', sans-serif;
        line-height: 1.15;
        overflow-wrap: anywhere;
        word-break: break-word;
    }}

    .util-card-title,
    .util-metric-label {{
        color: var(--util-muted);
        font-size: 0.79rem;
        text-transform: uppercase;
        letter-spacing: 0.14em;
        margin-bottom: 0.5rem;
    }}

    .util-card-copy,
    .util-metric-delta {{
        color: #c8d3eb;
        font-size: 0.93rem;
        line-height: 1.6;
    }}

    .util-spacer-sm {{
        height: 0.85rem;
    }}

    .util-spacer-xs {{
        height: 0.45rem;
    }}

    .util-spacer-md {{
        height: 1.2rem;
    }}

    .util-summary-divider {{
        height: 1px;
        margin: 0.35rem 0 1rem 0;
        background: linear-gradient(90deg, rgba(157, 180, 255, 0.22), rgba(157, 180, 255, 0.05));
        border-radius: 999px;
    }}

    .util-metric-value {{
        color: var(--util-text);
        font-size: 2rem;
        font-family: 'Space Grotesk', sans-serif;
        font-weight: 700;
        line-height: 1.05;
    }}

    div[data-baseweb="tab-list"] {{
        gap: 0.65rem;
        background:
            linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.012)),
            rgba(10, 14, 25, 0.76);
        border: 1px solid rgba(157, 180, 255, 0.14);
        padding: 0.48rem;
        border-radius: 24px;
        backdrop-filter: blur(18px);
        box-shadow: var(--util-shadow-soft);
    }}

    button[data-baseweb="tab"] {{
        background: transparent !important;
        color: var(--util-muted) !important;
        border-radius: 16px;
        padding: 0.72rem 1.08rem;
        border: 1px solid transparent;
        font-size: 0.95rem;
        transition: all 0.25s ease;
    }}

    button[data-baseweb="tab"]:hover {{
        color: var(--util-text) !important;
        background: rgba(255, 255, 255, 0.04) !important;
    }}

    button[data-baseweb="tab"][aria-selected="true"] {{
        background:
            linear-gradient(180deg, rgba(255,255,255,0.08), rgba(255,255,255,0.02)),
            linear-gradient(135deg, rgba(139, 92, 246, 0.26), rgba(88, 166, 255, 0.16)) !important;
        color: #f8fbff !important;
        border: 1px solid var(--util-border-strong) !important;
        box-shadow:
            inset 0 1px 0 rgba(255,255,255,0.07),
            0 12px 28px rgba(18, 24, 42, 0.34);
    }}

    div.stButton > button,
    div.stDownloadButton > button {{
        border-radius: 16px;
        font-weight: 700;
        padding: 0.72rem 1rem;
        min-height: 2.9rem;
        transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease, background 0.2s ease;
    }}

    div.stButton > button {{
        background:
            linear-gradient(180deg, rgba(255,255,255,0.08), rgba(255,255,255,0.02)),
            linear-gradient(135deg, var(--util-accent-violet) 0%, var(--util-accent-blue) 100%);
        color: white !important;
        border: 1px solid rgba(157, 180, 255, 0.28);
        box-shadow: 0 20px 38px rgba(34, 50, 95, 0.34);
    }}

    div.stButton > button:hover,
    div.stDownloadButton > button:hover {{
        transform: translateY(-1px);
    }}

    div.stButton > button:hover {{
        box-shadow: 0 24px 42px rgba(34, 50, 95, 0.4);
    }}

    div.stDownloadButton > button {{
        background:
            linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.01)),
            rgba(12, 16, 28, 0.86);
        color: var(--util-text-soft) !important;
        border: 1px solid rgba(157, 180, 255, 0.18);
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
        backdrop-filter: blur(12px);
    }}

    div.stDownloadButton > button:hover {{
        background:
            linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02)),
            rgba(16, 20, 34, 0.94);
        border-color: rgba(157, 180, 255, 0.3);
    }}

    div.stButton,
    div.stDownloadButton {{
        margin-top: 0.32rem;
        margin-bottom: 0.38rem;
    }}

    div[data-testid="stDataFrame"],
    div[data-testid="stAlert"] {{
        border-radius: 22px;
        overflow: hidden;
        box-shadow: var(--util-shadow-soft);
    }}

    div[data-testid="stDataFrame"] {{
        border: 1px solid rgba(157, 180, 255, 0.16);
        background:
            linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.01)),
            rgba(10, 13, 23, 0.9);
    }}

    div[data-testid="stDataFrame"] [role="grid"] {{
        background: transparent !important;
    }}

    div[data-testid="stAlert"] {{
        background:
            linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.01)),
            rgba(22, 15, 40, 0.86);
        border: 1px solid rgba(139, 92, 246, 0.24);
        color: var(--util-text);
    }}

    div[data-testid="stAlert"] svg {{
        fill: var(--util-accent-violet);
        color: var(--util-accent-violet);
    }}

    div[data-baseweb="select"] > div,
    div[data-baseweb="input"] > div,
    .stDateInput > div > div,
    .stNumberInput > div > div,
    .stTextInput > div > div,
    .stTextArea textarea {{
        background:
            linear-gradient(180deg, rgba(255,255,255,0.045), rgba(255,255,255,0.015)),
            rgba(11, 15, 26, 0.9) !important;
        border: 1px solid rgba(157, 180, 255, 0.18) !important;
        color: var(--util-text) !important;
        border-radius: 16px !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
    }}

    div[data-baseweb="select"] > div:hover,
    div[data-baseweb="input"] > div:hover,
    .stDateInput > div > div:hover,
    .stNumberInput > div > div:hover,
    .stTextInput > div > div:hover,
    .stTextArea textarea:hover {{
        border-color: rgba(157, 180, 255, 0.28) !important;
    }}

    .stRadio [role="radiogroup"] {{
        gap: 0.5rem;
        background:
            linear-gradient(180deg, rgba(255,255,255,0.035), rgba(255,255,255,0.01)),
            rgba(9, 13, 22, 0.76);
        padding: 0.4rem;
        border: 1px solid rgba(157, 180, 255, 0.12);
        border-radius: 18px;
        backdrop-filter: blur(16px);
    }}

    .stRadio [role="radiogroup"] label {{
        background: rgba(255,255,255,0.02);
        border: 1px solid transparent;
        border-radius: 14px;
        padding: 0.45rem 0.7rem;
    }}

    .stSlider [data-baseweb="slider"] {{
        padding-top: 0.8rem;
        padding-bottom: 0.45rem;
    }}

    .stSlider [data-baseweb="thumb"] {{
        background: linear-gradient(135deg, var(--util-accent-violet), var(--util-accent-blue)) !important;
        box-shadow: 0 0 0 6px rgba(139, 92, 246, 0.14);
    }}

    .stTabs {{
        margin-top: 0.55rem;
    }}

    [data-testid="stExpander"] {{
        background: rgba(10, 13, 23, 0.82);
        border-radius: 18px;
        border: 1px solid rgba(157, 180, 255, 0.12);
    }}

    @media (max-width: 1050px) {{
        .util-hero-grid,
        .util-callout-grid {{
            grid-template-columns: 1fr;
        }}

        .util-section-heading {{
            flex-direction: column;
            align-items: flex-start;
        }}
    }}

    @media (max-width: 900px) {{
        .util-hero {{
            padding: 1.2rem;
        }}
    }}
    </style>
    """.format(**tokens)


st.markdown(build_theme_css(THEME_TOKENS), unsafe_allow_html=True)

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


def render_section_shell_start(
    kicker: str,
    title: str,
    description: str,
    side_note: str | None = None,
):
    st.markdown(f'<div class="util-section-kicker">{kicker}</div>', unsafe_allow_html=True)
    st.markdown(f'<h2 class="util-section-title">{title}</h2>', unsafe_allow_html=True)
    st.markdown(f'<div class="util-section-description">{description}</div>', unsafe_allow_html=True)
    if side_note:
        st.markdown(f'<div class="util-side-note">{side_note}</div>', unsafe_allow_html=True)


def render_section_shell_end():
    return None


def render_info_card(title: str, body: str):
    st.markdown(
        f"""
        <div class="util-card">
            <div class="util-card-title">{title}</div>
            <div class="util-card-copy">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_callout_grid(items: list[tuple[str, str]], gap: str = "large"):
    if not items:
        return

    num_columns = 2 if len(items) > 1 else 1
    rows = [items[i:i + num_columns] for i in range(0, len(items), num_columns)]

    for row in rows:
        columns = st.columns(len(row), gap=gap)
        for column, (label, value) in zip(columns, row):
            with column:
                st.markdown(
                    f"""
                    <div class="util-callout">
                        <div class="util-callout-label">{label}</div>
                        <div class="util-callout-value">{value}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


def apply_estimator_value_to_optimizer():
    estimated_machine_watts = st.session_state.get("estimated_machine_watts")
    if estimated_machine_watts is not None:
        st.session_state["optimizer_machine_watts"] = int(estimated_machine_watts)


def infer_interval_minutes(df: pd.DataFrame) -> float:
    if len(df) < 2:
        return 60.0

    ts = pd.to_datetime(df["timestamp"]).sort_values()
    diffs = ts.diff().dropna()

    if diffs.empty:
        return 60.0

    return diffs.dt.total_seconds().median() / 60


def format_local_timestamp(value) -> str:
    ts = pd.to_datetime(value)
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_convert(APP_TIMEZONE)
    return ts.strftime("%b %d, %Y %I:%M %p")


def build_interval_transparency_df(
    optimized_df: pd.DataFrame,
    machine_watts: int,
) -> pd.DataFrame:
    df = add_interval_impact_columns(
        schedule_df=optimized_df,
        machine_watts=machine_watts,
        run_flag_column="run_flag",
    ).copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["local_time"] = df["timestamp"].apply(format_local_timestamp)
    df["interval_end_time"] = (
        df["timestamp"] + pd.to_timedelta(df["interval_hours"], unit="h")
    ).apply(format_local_timestamp)
    df["selected_by_optimizer"] = df["run_flag"].map({1: "Yes", 0: "No"})
    df["cleanest_rank"] = (
        df["interval_carbon_kg_if_run"].rank(method="dense", ascending=True).astype(int)
    )
    return df


def build_optimal_run_times_df(
    optimized_df: pd.DataFrame,
    machine_watts: int,
) -> pd.DataFrame:
    df = build_interval_transparency_df(optimized_df, machine_watts)
    df = df[df["run_flag"] == 1].copy()
    return df[
        [
            "local_time",
            "interval_end_time",
            "carbon_g_per_kwh",
            "interval_carbon_kg_if_run",
            "selected_by_optimizer",
        ]
    ].rename(
        columns={
            "local_time": "Local Time",
            "interval_end_time": "Interval End",
            "carbon_g_per_kwh": "Carbon Signal (g/kWh)",
            "interval_carbon_kg_if_run": "Estimated Carbon (kg)",
            "selected_by_optimizer": "Run Selected",
        }
    )


def build_eligible_intervals_export_df(
    optimized_df: pd.DataFrame,
    machine_watts: int,
) -> pd.DataFrame:
    df = build_interval_transparency_df(optimized_df, machine_watts)
    df = df[df["eligible_flag"] == 1].copy()
    return df[
        [
            "local_time",
            "carbon_g_per_kwh",
            "interval_carbon_kg_if_run",
            "selected_by_optimizer",
            "cleanest_rank",
        ]
    ].rename(
        columns={
            "local_time": "Local Time",
            "carbon_g_per_kwh": "Carbon Signal (g/kWh)",
            "interval_carbon_kg_if_run": "Estimated Carbon (kg)",
            "selected_by_optimizer": "Selected by Optimizer",
            "cleanest_rank": "Cleanest Rank",
        }
    )


@st.cache_data(show_spinner=False)
def build_historical_emissions_export_df(
    region: str,
    days: int = 14,
) -> pd.DataFrame:
    historical_df = build_live_historical_export_table(region=region, days=days).copy()
    historical_df = historical_df.sort_values("timestamp").reset_index(drop=True)
    historical_df["Local Time"] = historical_df["timestamp"].apply(format_local_timestamp)
    export_df = historical_df[
        ["Local Time", "carbon_g_per_kwh", "historical_region_used"]
    ].rename(
        columns={
            "carbon_g_per_kwh": "Carbon Signal (g/kWh)",
            "historical_region_used": "Historical Region Used",
        }
    )
    return export_df


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
        <div class="util-pill-row">
            <span class="{source_css}">Source: {source_label}</span>
            <span class="util-pill">Resolved Region: {region}</span>
            <span class="util-pill">Forecast Mode: {forecast_mode_label}</span>
            <span class="util-pill">Schedule Mode: {schedule_mode_label}</span>
            <span class="util-pill">Granularity: {interval_minutes:.0f} min</span>
            <span class="util-pill">Forecast Window: {forecast_min.strftime("%b %d %I:%M %p")} → {forecast_max.strftime("%b %d %I:%M %p")}</span>
            {extra_pills}
        </div>
        """,
        unsafe_allow_html=True
    )


def build_carbon_chart(display_df: pd.DataFrame) -> alt.Chart:
    chart_df = display_df.copy()
    chart_df["timestamp"] = pd.to_datetime(chart_df["timestamp"])
    chart_df["Selected"] = chart_df["run_flag"].apply(lambda x: "Selected" if x == 1 else "Not Selected")

    axis = alt.Axis(
        titleColor="#dbe7ff",
        labelColor="#a7b1c7",
        gridColor="rgba(157, 180, 255, 0.12)",
        domainColor="rgba(157, 180, 255, 0.16)",
        tickColor="rgba(157, 180, 255, 0.16)",
    )

    base = alt.Chart(chart_df).encode(
        x=alt.X("timestamp:T", title="Time", axis=axis),
        y=alt.Y("carbon_g_per_kwh:Q", title="Carbon Intensity (g/kWh)", axis=axis)
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
        color="#6ee7b7"
    )

    return (line + selected_points).properties(height=350).configure_view(
        strokeWidth=0
    ).configure(
        background="transparent"
    )


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
    ).properties(height=320).configure_view(
        strokeWidth=0
    ).configure(
        background="transparent"
    )


def build_price_chart(display_df: pd.DataFrame) -> alt.Chart:
    chart_df = display_df.copy()
    chart_df["timestamp"] = pd.to_datetime(chart_df["timestamp"])

    axis = alt.Axis(
        titleColor="#dbe7ff",
        labelColor="#a7b1c7",
        gridColor="rgba(157, 180, 255, 0.12)",
        domainColor="rgba(157, 180, 255, 0.16)",
        tickColor="rgba(157, 180, 255, 0.16)",
    )

    return alt.Chart(chart_df).mark_line(color="#8b5cf6", strokeWidth=3).encode(
        x=alt.X("timestamp:T", title="Time", axis=axis),
        y=alt.Y("price_per_kwh:Q", title="Price ($/kWh)", axis=axis),
        tooltip=[
            alt.Tooltip("timestamp:T", title="Time"),
            alt.Tooltip("price_per_kwh:Q", title="Price ($/kWh)", format=".3f")
        ]
    ).properties(height=300).configure_view(
        strokeWidth=0
    ).configure(
        background="transparent"
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


def render_recommendation_card(
    result: dict,
    schedule_df: pd.DataFrame,
    display_df: pd.DataFrame
):
    workload = result["workload_input"]
    run_window = build_run_window_summary(schedule_df)

    objective_label = "carbon emissions" if workload.objective == "carbon" else "electricity cost"

    st.subheader("Carbon Forecast with Recommended Intervals")
    st.altair_chart(build_carbon_chart(display_df), use_container_width=True)

    st.subheader("Electricity Price Forecast")
    st.altair_chart(build_price_chart(display_df), use_container_width=True)

    selected_rows = display_df[display_df["run_flag"] == 1][[
        "hour_label", "carbon_g_per_kwh", "price_per_kwh"
    ]].rename(columns={
        "hour_label": "Selected Run Time",
        "carbon_g_per_kwh": "Carbon (g/kWh)",
        "price_per_kwh": "Price ($/kWh)"
    })

    st.subheader("Selected Schedule")
    st.dataframe(selected_rows, use_container_width=True)

    st.subheader("Possible Schedule")

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

    render_info_card(
        "Recommendation",
        (
            f"Run your workload from <strong>{run_window['start']}</strong> to "
            f"<strong>{run_window['end']}</strong> to minimize <strong>{objective_label}</strong>."
            f"<br><br><strong>Selected Intervals:</strong> {run_window['intervals']}"
            f"<br><strong>Machine Wattage:</strong> {int(workload.machine_watts):,} W"
            f"<br><strong>Compute Hours Required:</strong> {int(workload.compute_hours_required)} hours"
        ),
    )


# ---------------------------------------------------
# Session State Defaults
# ---------------------------------------------------

if "estimated_machine_watts" not in st.session_state:
    st.session_state["estimated_machine_watts"] = None

if "optimizer_machine_watts" not in st.session_state:
    st.session_state["optimizer_machine_watts"] = 300

if "result" not in st.session_state:
    st.session_state["result"] = None

if "last_forecast_mode_label" not in st.session_state:
    st.session_state["last_forecast_mode_label"] = "Live Carbon"

if "last_schedule_mode_label" not in st.session_state:
    st.session_state["last_schedule_mode_label"] = "Flexible"

FORECAST_MODE_LABEL = "Live Carbon"
FORECAST_MODE = "live_carbon"

# ---------------------------------------------------
# Header
# ---------------------------------------------------

st.markdown(
    """
    <div class="util-hero">
        <div class="util-hero-grid">
            <div class="util-brand-row">
                <div class="util-logo-shell">
                    <img src="data:image/png;base64,{}" width="40" />
                </div>
                <div>
                    <h1 class="util-brand-title">Util</h1>
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

tab_labels = [
    "Optimizer",
    "Savings Analysis",
    "Forecast Signals",
    "Run Timeline",
    "Power Estimator",
    "Multi-Location",
    "About Util",
]
tabs = st.tabs(tab_labels)
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = tabs

# ====================================================
# TAB 1 — OPTIMIZER
# ====================================================

with tab1:
    render_section_shell_start(
        kicker="Optimization Studio",
        title="Workload Optimization",
        description=(
            "Set workload constraints, choose how Util estimates carbon signals, and generate a "
            "recommended run window without changing the optimization logic underneath."
        ),
    )

    col_input, col_output = st.columns([1, 2], gap="large")

    with col_input:
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
        st.caption(
            "Flexible is optimal 5 minute chunks. Continuous Block selects one continuous run window for the requested duration."
        )

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

        estimated_machine_watts = st.session_state.get("estimated_machine_watts")
        if estimated_machine_watts is None:
            render_info_card(
                "Input Tip",
                "Use the Power Estimator tab to generate a machine wattage recommendation, then copy it here only when you want to."
            )
        else:
            render_info_card(
                "Estimator Recommendation",
                (
                    f"Latest estimator recommendation: <strong>{int(estimated_machine_watts):,} W</strong>."
                    " Your optimizer input stays editable and will only change if you use the estimator button."
                ),
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
                forecast_mode = FORECAST_MODE
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
                st.session_state["last_forecast_mode_label"] = FORECAST_MODE_LABEL
                st.session_state["last_schedule_mode_label"] = schedule_mode_label
                if forecast_mode == "live_carbon":
                    st.session_state["watttime_token_available"] = True
                    st.session_state["last_successful_api_pull_time"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

                if ANALYTICS_LOGGING_ENABLED:
                    analytics_record = build_run_analytics_record(
                        result=result,
                        run_type=DEFAULT_ANALYTICS_RUN_TYPE,
                        schedule_mode_label=schedule_mode_label,
                        forecast_mode_label=FORECAST_MODE_LABEL,
                        api_mode=FORECAST_MODE,
                    )
                    append_run(ANALYTICS_PATH, analytics_record)

            except Exception as e:
                error_message = str(e)

                if forecast_mode == "live_carbon" and "WattTime" in error_message:
                    st.session_state["watttime_token_available"] = False
                    st.error(
                        "Live carbon is currently unavailable because WattTime authentication "
                        "or API access failed. Please update deployment secrets/API plan."
                    )
                elif isinstance(e, InfeasibleScheduleError):
                    st.error(INFEASIBLE_WORKLOAD_MESSAGE)
                else:
                    st.error("An error occurred while running the pipeline.")
                    st.exception(e)

        result = st.session_state["result"]

        if result is None:
            st.markdown('<div class="util-spacer-sm"></div>', unsafe_allow_html=True)
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

            st.markdown('<div class="util-spacer-xs"></div>', unsafe_allow_html=True)
            render_callout_grid(
                [
                    ("Mapped Region", region),
                    ("Objective", result["workload_input"].objective.title()),
                    ("Machine Wattage", f"{int(result['workload_input'].machine_watts):,} W"),
                    ("Selected Intervals", str(run_window["intervals"])),
                ],
                gap="medium",
            )

            st.markdown('<div class="util-summary-divider"></div>', unsafe_allow_html=True)
            render_info_card(
                "Recommended Window",
                (
                    f"Run from <strong>{run_window['start']}</strong> to "
                    f"<strong>{run_window['end']}</strong> based on the selected forecast "
                    f"and scheduling strategy."
                ),
            )

            st.markdown('<div class="util-spacer-xs"></div>', unsafe_allow_html=True)
            k1, k2, k3, k4 = st.columns(4, gap="medium")
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

            st.markdown('<div class="util-spacer-sm"></div>', unsafe_allow_html=True)
            st.subheader("Compared with Running Immediately")

            st.markdown('<div class="util-spacer-xs"></div>', unsafe_allow_html=True)
            rn1, rn2 = st.columns(2, gap="medium")
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

            st.markdown('<div class="util-spacer-sm"></div>', unsafe_allow_html=True)
            st.subheader("Recommended Schedule")

            st.markdown('<div class="util-spacer-xs"></div>', unsafe_allow_html=True)
            if selected_schedule.empty:
                st.warning("No run intervals were selected.")
            else:
                st.dataframe(build_selected_schedule_df(schedule), use_container_width=True)

    render_section_shell_end()

# ====================================================
# TAB 2 — SAVINGS ANALYSIS
# ====================================================

with tab2:
    render_section_shell_start(
        kicker="Impact Review",
        title="Savings Analysis",
        description="",
    )

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
        optimal_run_times_export_df = build_optimal_run_times_df(
            optimized_df=optimized,
            machine_watts=int(workload.machine_watts),
        )
        eligible_intervals_export_df = build_eligible_intervals_export_df(
            optimized_df=optimized,
            machine_watts=int(workload.machine_watts),
        )
        savings_csv = savings_export_df.to_csv(index=False).encode("utf-8")
        optimal_run_times_csv = optimal_run_times_export_df.to_csv(index=False).encode("utf-8")
        eligible_intervals_csv = eligible_intervals_export_df.to_csv(index=False).encode("utf-8")
        historical_emissions_csv = None
        historical_export_error = None

        if st.session_state.get("last_forecast_mode_label") == "Live Carbon":
            try:
                historical_emissions_export_df = build_historical_emissions_export_df(
                    region=result["region"],
                    days=14,
                )
                historical_emissions_csv = historical_emissions_export_df.to_csv(index=False).encode("utf-8")
            except Exception as exc:
                historical_export_error = str(exc)

        export_col1, export_col2, export_col3, export_col4 = st.columns(4)
        with export_col1:
            st.download_button(
                "Download Savings Summary CSV",
                data=savings_csv,
                file_name="util_savings_analysis.csv",
                mime="text/csv",
            )
        with export_col2:
            st.download_button(
                "Download Optimal Run Times CSV",
                data=optimal_run_times_csv,
                file_name="util_optimal_run_times.csv",
                mime="text/csv",
            )
        with export_col3:
            st.download_button(
                "Download Eligible Intervals CSV",
                data=eligible_intervals_csv,
                file_name="util_eligible_intervals.csv",
                mime="text/csv",
            )
        with export_col4:
            if historical_emissions_csv is not None:
                st.download_button(
                    "Download 2-Week Historical CSV",
                    data=historical_emissions_csv,
                    file_name="util_historical_emissions_14_days.csv",
                    mime="text/csv",
                )
            elif st.session_state.get("last_forecast_mode_label") == "Live Carbon":
                st.caption("2-week historical CSV unavailable for this region/API access.")
            else:
                st.caption("2-week historical CSV is available in Live Carbon mode.")

        if historical_export_error:
            st.caption(f"Historical export note: {historical_export_error}")

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

        st.caption(
            "Carbon estimates are based on marginal emissions (MOER), which measure the impact of "
            "additional electricity demand on the grid. Values near zero indicate periods where extra "
            "load does not increase total emissions, often due to excess renewable energy."
        )

    render_section_shell_end()

# ====================================================
# TAB 3 — FORECAST SIGNALS
# ====================================================

with tab3:
    render_section_shell_start(
        kicker="Signal View",
        title="Forecast Signals",
        description=(
            "Inspect the carbon and price curves that drive the recommendation, with the same "
            "selection logic and datasets shown in a more cinematic charting environment."
        ),
    )

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

        render_recommendation_card(result, result["schedule"], display_df)
        render_location_access_card(result)

    render_section_shell_end()

# ====================================================
# TAB 4 — RUN TIMELINE
# ====================================================

with tab4:
    render_section_shell_start(
        kicker="Execution Map",
        title="Run Timeline",
        description=(
            "See the full interval-by-interval plan for the recommended schedule without losing "
            "any of the existing timeline data."
        ),
    )

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

    render_section_shell_end()

# ====================================================
# TAB 5 — POWER ESTIMATOR
# ====================================================

with tab5:
    render_section_shell_start(
        kicker="Power Model",
        title="System Power Estimator",
        description=(
            "Estimate approximate system power draw under compute load when you do not know your "
            "machine wattage directly."
        ),
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
    st.session_state["estimated_machine_watts"] = estimated_power
    estimated_kwh_per_hour = estimated_power / 1000

    p1, p2, p3 = st.columns(3)
    with p1:
        render_metric_card("Estimated Load", f"{estimated_power:,} W")
    with p2:
        render_metric_card("Energy Per Hour", f"{estimated_kwh_per_hour:.2f} kWh")
    with p3:
        render_metric_card("GPU Power Component", f"{int(gpu_total_estimated):,} W")

    render_info_card(
        "Estimator Recommendation",
        (
            f"The current estimator recommends <strong>{estimated_power:,} W</strong>."
            " This stays separate from the optimizer until you explicitly copy it over."
        ),
    )

    if st.button("Use estimator value in optimizer", on_click=apply_estimator_value_to_optimizer):
        st.success(
            f"Copied {estimated_power:,} W into the optimizer input."
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

    render_section_shell_end()

# ====================================================
# TAB 6 — MULTI-LOCATION
# ====================================================

with tab6:
    render_section_shell_start(
        kicker="Location Scan",
        title="Multi-Location",
        description=(
            "Compare the same workload across multiple ZIP codes and surface the strongest "
            "cost and carbon outcomes in the same glassy product language."
        ),
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
                forecast_mode = FORECAST_MODE
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
                        "or API access failed. Please update deployment secrets/API plan."
                    )
                elif isinstance(e, InfeasibleScheduleError):
                    st.error(INFEASIBLE_WORKLOAD_MESSAGE)
                else:
                    st.error("An error occurred while comparing locations.")
                    st.exception(e)

    render_section_shell_end()

# ====================================================
# TAB 7 — ABOUT
# ====================================================

with tab7:
    render_section_shell_start(
        kicker="Product Context",
        title="About Util",
        description=(
            "A concise overview of what the product does today and where the roadmap can expand next."
        ),
    )

    render_info_card(
        "Current Product",
        (
            "<strong>Util</strong> is a compute scheduling and optimization product designed to help users "
            "run workloads at the best possible times and locations in order to minimize electricity costs "
            "and carbon emissions.<br><br>"
            "The current MVP is recommendation-only. It does not yet automatically control workloads or "
            "locations. Instead, it shows users when to run, how much they can save, what forecast signals "
            "drive the recommendation, and how much power their system is likely using."
        ),
    )
    render_callout_grid(
        [
            ("Live Carbon APIs", "Complete"),
            ("Electricity Pricing APIs", "Planned"),
            ("System Auto-Detection", "Planned"),
            ("Automated Control", "Planned"),
        ]
    )
    render_info_card(
        "Future Expansion",
        (
            "Future versions can add live telemetry, multi-region scheduling, and deeper partnerships with "
            "electricity providers to solve the issue from the supply side as well as the workload side."
        ),
    )

    render_section_shell_end()
