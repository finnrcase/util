# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

**Util** is a cost & carbon-aware compute scheduling optimizer. Users provide a workload (ZIP code, compute hours, deadline, machine power draw), and the system finds the cheapest or cleanest time windows to run it by combining real-time electricity prices and grid carbon intensity forecasts.

## Commands

### Python Backend
```bash
pip install -r requirements.txt

# Streamlit UI
streamlit run app.py

# FastAPI backend — must be launched from project root so src.* imports resolve
python -m uvicorn src.api.main:app --reload --host 127.0.0.1 --port 8000
```

### Frontend (React + TypeScript)
```bash
cd frontend
npm install
npm run dev          # Vite dev server
npm run build        # Production build
npm run tauri:dev    # Tauri desktop app (dev)
npm run tauri:build  # Tauri desktop app (release)
```

### Tests
```bash
pytest tests/                       # All tests
pytest tests/test_optimizer.py      # Single test file
```

## Environment

Requires a `.env` file at project root. Copy `.env.example` to get started. Key variables:

| Variable | Required | Purpose |
|---|---|---|
| `WATTTIME_USERNAME`, `WATTTIME_PASSWORD` | Yes (live mode) | Carbon intensity data |
| `ANTHROPIC_API_KEY` | Yes (AI summary) | Claude API for AI Decision Summary |
| `AI_SUMMARY_ENABLED` | No (default `true`) | Toggle AI summary feature |
| `AI_SUMMARY_MODEL` | No (default `claude-haiku-4-5-20251001`) | Model used for summaries |
| `AI_SUMMARY_RATE_LIMIT` | No (default `10`) | Requests per 60s per IP |
| `UTIL_API_BASE_URL` | Deployed Streamlit only | Backend URL for Streamlit→FastAPI calls; defaults to `http://127.0.0.1:8000` |
| `UTIL_SHOW_AI_DEBUG` | No (default `false`) | Show AI debug panel in Streamlit UI |

`get_setting()` in `src/runtime_config.py` resolves variables in this order: `os.environ` → `.env` (loaded at import time via `load_dotenv`) → Streamlit Secrets → supplied default.

## Architecture

### Request Lifecycle
```
Input → Location Resolution → Data Fetch → Optimization → Schedule → Metrics → Export
```

1. User supplies ZIP code, compute hours, deadline, machine wattage
2. ZIP resolves to a WattTime region (`src/location/`)
3. Carbon intensity + electricity prices are loaded (`src/data_fetcher.py`)
4. A baseline "run now" schedule is built (`src/baseline.py`)
5. The constrained optimizer picks the best windows (`src/optimizer.py`)
6. Output is formatted into a schedule, metrics, and optional CSV export

### Core Engine (`src/`)
| File | Role |
|------|------|
| `pipeline.py` | Orchestrates the full optimization flow end-to-end |
| `optimizer.py` | `scipy.optimize`-based constrained solver |
| `data_fetcher.py` | Loads and prepares carbon/price forecast data |
| `baseline.py` | Builds the naive "run immediately" comparison schedule |
| `scheduler.py` | Formats solver output into human-readable schedules |
| `metrics.py` | Computes cost/carbon savings vs baseline |
| `price_router.py` | Selects the right regional price adapter |

### Pricing Adapters (`src/price_adapters/`)
One adapter per electricity market: CAISO (California), ERCOT (Texas), PJM, MISO. All extend `base.py`.

### Optimization Modes
- **Schedule types**: `flexible` (best individual intervals) vs `block` (single continuous window)
- **Objectives**: `carbon`, `cost`, or `balanced` (weighted combination). Must be exactly one of these strings — UI display labels (`"Minimize Carbon"` etc.) must be mapped to backend values before passing to the pipeline.
- **Estimation**: `forecast_only` or `forecast_plus_historical_expectation`

### Forecasting (`src/forecasting/`)
Short-horizon forecasts are extended using ML pattern matching (`pattern_extension.py`) and blended with historical data (`carbon_blender.py`). In demo mode, sample CSVs under `data/raw/` are used instead of live API calls.

### AI Decision Summary (`src/services/ai/`)
Strictly additive layer — does not affect optimizer logic or scheduling. Architecture:

- `POST /api/v1/ai/interpret` (`src/api/routes/ai.py`) — rate-limited FastAPI route
- `ai_service.py` — calls Anthropic API, returns `AiInterpretResponse`; never raises, always falls back to `status="unavailable"`
- `schemas.py` — `AiInterpretRequest` / `AiInterpretResponse`; decoupled from optimizer schemas
- `prompts.py` — structured system prompt with judgment criteria (`tradeoff_strength`, `decision_confidence`, `objective_driver`, `alternative_attractiveness`) + single `summary` paragraph
- `streamlit_client.py` — Streamlit-side HTTP client; resolves backend URL via `st.secrets["UTIL_API_BASE_URL"]` → `os.getenv` → localhost fallback

The React frontend calls the same endpoint via `frontend/src/lib/api.ts:interpretOptimization()`. Both surfaces use the same backend AI layer with different UI wrappers only.

Provider timeout is 30 seconds (`_PROVIDER_TIMEOUT_SECONDS` in `ai_service.py`). All logs use bracketed tags (`[AI-3-CONFIG]`, `[AI-8]`, etc.) for easy grepping in Render logs.

### Multiple Interfaces
The same Python optimization engine is exposed through four surfaces:
- **Streamlit app** (`app.py`) — interactive dashboard; the Streamlit process must also have the FastAPI backend reachable for the AI summary feature
- **FastAPI REST API** (`src/api/main.py`) — programmatic access; schemas in `src/api/schemas.py`
- **React frontend** (`frontend/src/`) — modern web UI with TanStack Query and React Hook Form
- **Tauri desktop app** (`frontend/src-tauri/`) — native wrapper around the React frontend; Rust sidecar launcher in `scripts/backend_sidecar.py`

### Location Resolution (`src/location/`)
`zip_resolver.py` → `region_resolver.py` → `location_service.py` (WattTime region lookup). Demo mode uses `data/raw/zip_to_region_sample.csv`.

### Multi-Location Analysis (`src/analysis/multi_location.py`)
`run_multi_location_analysis()` runs the full pipeline once per ZIP and returns `(summary_df, timeseries)` — a scalar results DataFrame plus per-ZIP time series dicts containing `timestamp`, `price_per_kwh`, `carbon_g_per_kwh`, and `run_flag` from `result["schedule"]`. The Streamlit tab uses this to render overlay charts without additional pipeline runs.
