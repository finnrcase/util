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

# FastAPI backend
uvicorn src.api.main:app --reload
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

Requires a `.env` file at project root. Required variables:
- `WATTTIME_USERNAME`, `WATTTIME_PASSWORD` — carbon intensity data source

Optional: AWS S3 credentials for export storage. See `src/runtime_config.py` for the full list and fallback order (env vars → `.env` → Streamlit Secrets).

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
- **Objectives**: `carbon`, `cost`, or `balanced` (weighted combination)
- **Estimation**: `forecast_only` or `forecast_plus_historical_expectation`

### Forecasting (`src/forecasting/`)
Short-horizon forecasts are extended using ML pattern matching (`pattern_extension.py`) and blended with historical data (`carbon_blender.py`). In demo mode, sample CSVs under `data/raw/` are used instead of live API calls.

### Multiple Interfaces
The same Python optimization engine is exposed through four surfaces:
- **Streamlit app** (`app.py`) — interactive dashboard
- **FastAPI REST API** (`src/api/main.py`) — programmatic access; schemas in `src/api/schemas.py`
- **React frontend** (`frontend/src/`) — modern web UI with TanStack Query and React Hook Form
- **Tauri desktop app** (`frontend/src-tauri/`) — native wrapper around the React frontend; Rust sidecar launcher in `scripts/backend_sidecar.py`

### Location Resolution (`src/location/`)
`zip_resolver.py` → `region_resolver.py` → `location_service.py` (WattTime region lookup). Demo mode uses `data/raw/zip_to_region_sample.csv`.
