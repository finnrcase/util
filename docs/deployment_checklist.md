## Util Deployment Checklist

Use this checklist to keep local and Streamlit deployments aligned.

### Required secrets / environment values

- `WATTTIME_USERNAME`
- `WATTTIME_PASSWORD`

### Optional config flags that should match between local and deployment

- `APP_MODE`
- `UTIL_ANALYTICS_ENABLED`
- `UTIL_ANALYTICS_RUN_TYPE`
- `UTIL_SHOW_RUNTIME_DIAGNOSTICS`
- `UTIL_ADMIN_PASSWORD`
- `UTIL_CARBON_PRICE_USD_PER_TON`
- `UTIL_CLEAN_ENERGY_CREDIT_USD`
- `UTIL_ELECTRICITY_PRICE_ADDER_PCT`

### Required tracked files

- `assets/logo/util_logo.png`
- `data/raw/zip_to_region_sample.csv`
- `data/raw/sample_carbon_forecast.csv`
- `data/raw/sample_price_forecast.csv`

### Dependency parity

- Deploy from the same committed `requirements.txt`
- Confirm `streamlit`, `altair`, `pandas`, `numpy`, `requests`, `python-dotenv`, and `pgeocode` install successfully

### Runtime behavior parity

- Local `.env` and Streamlit secrets should contain the same keys for any enabled integration
- The app now normalizes scheduling defaults to `America/Los_Angeles`; deployment should not rely on server-local timezone
- Cold-start deployment may not have local caches; first-run latency can be higher, but results should remain consistent
- Analytics/export files are created at runtime; they should not be treated as required seeded deployment assets
