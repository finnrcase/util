from __future__ import annotations

import io
from datetime import timedelta
from functools import lru_cache
from typing import Any

import pandas as pd
import requests

from src.price_adapters.base import PriceProviderError, finalize_normalized_price_frame
from src.runtime_config import get_setting
from src.scheduling_window import APP_TIMEZONE


PJM_DA_LMPS_URL = "https://api.pjm.com/api/v1/da_hrl_lmps"
PJM_PRICE_SOURCE_LABEL = "PJM"
PJM_MARKET_LABEL = "DAY_AHEAD"
PJM_EASTERN_TIMEZONE = "America/New_York"
PJM_RETRYABLE_STATUS_CODES = {429}
PJM_KEY_SETTING_NAMES = (
    "PJM_SUBSCRIPTION_KEY",
    "PJM_API_KEY",
    "PJM_DATAMINER_SUBSCRIPTION_KEY",
)


class PjmPricingError(PriceProviderError):
    """Raised when PJM pricing data cannot be fetched or normalized."""


def _get_pjm_subscription_key() -> str:
    for setting_name in PJM_KEY_SETTING_NAMES:
        value = str(get_setting(setting_name, "") or "").strip()
        if value:
            return value

    searched_names = ", ".join(PJM_KEY_SETTING_NAMES)
    raise PjmPricingError(
        f"PJM pricing requires a Data Miner subscription key. Set one of: {searched_names}."
    )


def _format_pjm_datetime_range(start_time: Any, end_time: Any) -> str:
    start_ts = pd.to_datetime(start_time, errors="coerce")
    end_ts = pd.to_datetime(end_time, errors="coerce")
    if pd.isna(start_ts) or pd.isna(end_ts):
        raise PjmPricingError("PJM pricing requires valid start and end timestamps.")

    if getattr(start_ts, "tzinfo", None) is None:
        start_ts = start_ts.tz_localize(APP_TIMEZONE)
    else:
        start_ts = start_ts.tz_convert(APP_TIMEZONE)

    if getattr(end_ts, "tzinfo", None) is None:
        end_ts = end_ts.tz_localize(APP_TIMEZONE)
    else:
        end_ts = end_ts.tz_convert(APP_TIMEZONE)

    request_start = start_ts.floor("h").tz_convert(PJM_EASTERN_TIMEZONE)
    request_end = (end_ts.ceil("h") + timedelta(hours=1)).tz_convert(PJM_EASTERN_TIMEZONE)
    if request_end <= request_start:
        request_end = request_start + timedelta(hours=1)

    def _fmt(value: pd.Timestamp) -> str:
        return f"{value.month}-{value.day}-{value.year} {value.hour:02d}:{value.minute:02d}"

    return f"{_fmt(request_start)} to {_fmt(request_end)}"


def _build_pjm_request_params(
    *,
    node_or_zone: str,
    start_time: Any,
    end_time: Any,
) -> dict[str, str | int]:
    return {
        "rowCount": 50000,
        "startRow": 1,
        "sort": "datetime_beginning_ept",
        "order": "Asc",
        "datetime_beginning_ept": _format_pjm_datetime_range(start_time, end_time),
        "zone": node_or_zone,
        "type": "ZONE",
        "row_is_current": "TRUE",
        "format": "csv",
    }


def _normalize_pjm_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [str(column).strip().lower() for column in normalized.columns]
    return normalized


def _normalize_pjm_lmp_dataframe(
    raw_df: pd.DataFrame,
    *,
    region_code: str,
    node_or_zone: str,
) -> pd.DataFrame:
    df = _normalize_pjm_columns(raw_df)

    required_columns = {"datetime_beginning_utc", "total_lmp_da"}
    if not required_columns.issubset(df.columns):
        raise PjmPricingError(
            f"PJM response is missing required columns {sorted(required_columns)}. "
            f"Columns received: {list(df.columns)}"
        )

    if "zone" in df.columns:
        zone_match = df["zone"].astype(str).str.upper() == str(node_or_zone).strip().upper()
        if zone_match.any():
            df = df.loc[zone_match].copy()

    if "type" in df.columns:
        type_match = df["type"].astype(str).str.upper() == "ZONE"
        if type_match.any():
            df = df.loc[type_match].copy()

    if "pnode_name" in df.columns:
        zone_name_match = df["pnode_name"].astype(str).str.upper() == str(node_or_zone).strip().upper()
        if zone_name_match.any():
            df = df.loc[zone_name_match].copy()

    timestamp_series = pd.to_datetime(df["datetime_beginning_utc"], utc=True, errors="coerce")
    price_series = pd.to_numeric(df["total_lmp_da"], errors="coerce")

    normalized = pd.DataFrame(
        {
            "timestamp": timestamp_series.dt.tz_convert(APP_TIMEZONE).dt.tz_localize(None),
            "local_time": timestamp_series.dt.tz_convert(APP_TIMEZONE).dt.strftime("%Y-%m-%d %H:%M:%S"),
            "price_per_mwh": price_series,
            "price_per_kwh": price_series / 1000.0,
            "source_market": PJM_MARKET_LABEL,
            "source_provider": PJM_PRICE_SOURCE_LABEL,
            "node_or_zone": node_or_zone,
            "interval_minutes": 60.0,
            "price_type": "day_ahead_lmp",
            "is_forecast_or_historical": "forecast",
            "is_live_market_data": True,
            "source": PJM_PRICE_SOURCE_LABEL,
            "region_code": region_code,
            "price_node": node_or_zone,
        }
    )
    normalized = normalized.dropna(subset=["timestamp", "price_per_kwh"])
    normalized = normalized.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)

    if normalized.empty:
        raise PjmPricingError(
            f"PJM pricing normalization produced no usable rows for zone '{node_or_zone}'."
        )

    return finalize_normalized_price_frame(normalized)


def fetch_pjm_prices(
    *,
    region_code: str,
    node_or_zone: str,
    start_time: Any,
    end_time: Any,
    market: str,
    timeout_seconds: int = 45,
) -> pd.DataFrame:
    if str(market).strip().upper() not in {"DAY_AHEAD", "DAM"}:
        raise PjmPricingError(
            f"PJM provider currently supports day-ahead pricing only. Received market '{market}'."
        )

    params = _build_pjm_request_params(
        node_or_zone=node_or_zone,
        start_time=start_time,
        end_time=end_time,
    )
    cache_key = (
        region_code,
        node_or_zone,
        str(params["datetime_beginning_ept"]),
        str(params["type"]),
        str(params["row_is_current"]),
    )
    cached_rows = _fetch_pjm_prices_cached(
        cache_key=cache_key,
        timeout_seconds=timeout_seconds,
    )
    return pd.DataFrame(
        cached_rows,
        columns=[
            "timestamp",
            "local_time",
            "price_per_mwh",
            "price_per_kwh",
            "source_market",
            "source_provider",
            "node_or_zone",
            "interval_minutes",
            "price_type",
            "is_forecast_or_historical",
            "is_live_market_data",
            "source",
            "region_code",
            "price_node",
        ],
    )


@lru_cache(maxsize=32)
def _fetch_pjm_prices_cached(
    *,
    cache_key: tuple[str, str, str, str, str],
    timeout_seconds: int,
) -> tuple[tuple[pd.Timestamp, str, float, float, str, str, str, float, str, str, bool, str, str, str], ...]:
    region_code, node_or_zone, datetime_beginning_ept, pjm_type, row_is_current = cache_key
    params = {
        "rowCount": 50000,
        "startRow": 1,
        "sort": "datetime_beginning_ept",
        "order": "Asc",
        "datetime_beginning_ept": datetime_beginning_ept,
        "zone": node_or_zone,
        "type": pjm_type,
        "row_is_current": row_is_current,
        "format": "csv",
    }
    headers = {
        "Ocp-Apim-Subscription-Key": _get_pjm_subscription_key(),
        "Accept": "text/csv",
    }

    try:
        response = requests.get(
            PJM_DA_LMPS_URL,
            params=params,
            headers=headers,
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        raise PjmPricingError(f"PJM network request failed: {exc}") from exc

    print(f"[PJM DEBUG] Request URL: {response.url}")
    print(f"[PJM DEBUG] Response status: {response.status_code}")
    print(
        "[PJM DEBUG] Route metadata:",
        {
            "region_code": region_code,
            "zone": node_or_zone,
            "market": PJM_MARKET_LABEL,
            "request_range_ept": datetime_beginning_ept,
        },
    )

    if response.status_code in PJM_RETRYABLE_STATUS_CODES:
        raise PjmPricingError(
            f"PJM request was rate-limited with status {response.status_code}. Response preview: {response.text[:500]}"
        )

    if response.status_code >= 400:
        raise PjmPricingError(
            f"PJM request failed with status {response.status_code}. Response preview: {response.text[:500]}"
        )

    try:
        raw_df = pd.read_csv(io.StringIO(response.text))
    except Exception as exc:  # pragma: no cover - defensive parse guard
        raise PjmPricingError(f"PJM CSV parsing failed: {exc}") from exc

    normalized_df = _normalize_pjm_lmp_dataframe(
        raw_df,
        region_code=region_code,
        node_or_zone=node_or_zone,
    )
    print(f"[PJM DEBUG] Parsed rows: {len(normalized_df)}")
    print(f"[PJM DEBUG] Output sample:\n{normalized_df.head(3).to_string(index=False)}")
    return tuple(
        (
            row.timestamp,
            row.local_time,
            float(row.price_per_mwh),
            float(row.price_per_kwh),
            row.source_market,
            row.source_provider,
            row.node_or_zone,
            float(row.interval_minutes) if pd.notna(row.interval_minutes) else 0.0,
            row.price_type,
            row.is_forecast_or_historical,
            bool(row.is_live_market_data),
            row.source,
            row.region_code,
            row.price_node,
        )
        for row in normalized_df.itertuples(index=False)
    )
