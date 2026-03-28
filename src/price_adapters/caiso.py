from __future__ import annotations

import io
import logging
import time
import zipfile
from datetime import timedelta
from functools import lru_cache
from typing import Any

import pandas as pd
import requests

from src.price_adapters.base import finalize_normalized_price_frame
from src.scheduling_window import APP_TIMEZONE


CAISO_OASIS_SINGLE_ZIP_URL = "https://oasis.caiso.com/oasisapi/SingleZip"
CAISO_PRICE_SOURCE_LABEL = "CAISO"
CAISO_RETRYABLE_STATUS_CODES = {429}
CAISO_DEFAULT_TIMEOUT_SECONDS = 4
CAISO_DEFAULT_MAX_RETRY_ATTEMPTS = 1
CAISO_DEFAULT_RETRY_SLEEP_SECONDS = 0.5
caiso_logger = logging.getLogger("uvicorn.error")


class CaisoPricingError(ValueError):
    """Raised when CAISO pricing data cannot be fetched or normalized."""


def _format_oasis_timestamp(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        raise CaisoPricingError(f"Invalid CAISO timestamp boundary: {value}")

    if getattr(ts, "tzinfo", None) is None:
        ts = ts.tz_localize(APP_TIMEZONE)
    else:
        ts = ts.tz_convert(APP_TIMEZONE)

    return ts.tz_convert("UTC").strftime("%Y%m%dT%H:%M-0000")


def _extract_first_csv(zip_bytes: bytes) -> pd.DataFrame:
    try:
        archive = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as exc:
        response_preview = zip_bytes[:240].decode("utf-8", errors="ignore")
        raise CaisoPricingError(
            f"CAISO OASIS did not return a valid ZIP payload. Response preview: {response_preview}"
        ) from exc

    with archive:
        members = archive.namelist()
        if not members:
            raise CaisoPricingError("CAISO OASIS returned an empty ZIP archive.")

        csv_members = [name for name in members if name.lower().endswith(".csv")]
        xml_members = [name for name in members if name.lower().endswith(".xml")]

        if csv_members:
            with archive.open(csv_members[0]) as handle:
                return pd.read_csv(handle)

        if xml_members:
            with archive.open(xml_members[0]) as handle:
                error_text = handle.read().decode("utf-8", errors="ignore")
            raise CaisoPricingError(
                f"CAISO OASIS returned an XML response instead of CSV: {error_text[:500]}"
            )

    raise CaisoPricingError("CAISO OASIS response did not contain a CSV payload.")


def _normalize_oasis_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [str(column).strip().upper() for column in normalized.columns]
    return normalized


def _normalize_caiso_lmp_dataframe(
    raw_df: pd.DataFrame,
    *,
    price_node: str,
    region_code: str,
) -> pd.DataFrame:
    df = _normalize_oasis_columns(raw_df)

    timestamp_column = None
    for candidate in ["INTERVALSTARTTIME_GMT", "STARTTIME_GMT", "INTERVAL_END_GMT", "OPR_DT"]:
        if candidate in df.columns:
            timestamp_column = candidate
            break

    if timestamp_column is None:
        raise CaisoPricingError(
            f"Could not find a usable CAISO timestamp column in: {list(df.columns)}"
        )

    if "XML_DATA_ITEM" in df.columns:
        filtered = df[df["XML_DATA_ITEM"].astype(str).str.upper() == "LMP_PRC"].copy()
        if filtered.empty:
            raise CaisoPricingError("CAISO OASIS response did not include LMP_PRC rows.")
        if "MW" in filtered.columns:
            price_series = pd.to_numeric(filtered["MW"], errors="coerce")
        elif "VALUE" in filtered.columns:
            price_series = pd.to_numeric(filtered["VALUE"], errors="coerce")
        else:
            raise CaisoPricingError(
                f"CAISO LMP rows did not include an MW or VALUE column: {list(filtered.columns)}"
            )
        timestamp_series = pd.to_datetime(filtered[timestamp_column], utc=True, errors="coerce")
    elif "LMP_PRC" in df.columns:
        filtered = df.copy()
        price_series = pd.to_numeric(filtered["LMP_PRC"], errors="coerce")
        timestamp_series = pd.to_datetime(filtered[timestamp_column], utc=True, errors="coerce")
    else:
        raise CaisoPricingError(
            f"Could not find CAISO LMP columns in: {list(df.columns)}"
        )

    normalized = pd.DataFrame(
        {
            "timestamp": timestamp_series.dt.tz_convert(APP_TIMEZONE).dt.tz_localize(None),
            "local_time": timestamp_series.dt.tz_convert(APP_TIMEZONE).dt.strftime("%Y-%m-%d %H:%M:%S"),
            "price_per_mwh": price_series,
            "price_per_kwh": price_series / 1000.0,
            "source_market": "DAM",
            "source_provider": CAISO_PRICE_SOURCE_LABEL,
            "node_or_zone": price_node,
            "interval_minutes": 60.0,
            "price_type": "day_ahead_lmp",
            "is_forecast_or_historical": "forecast",
            "is_live_market_data": True,
            "source": CAISO_PRICE_SOURCE_LABEL,
            "region_code": region_code,
            "price_node": price_node,
        }
    )
    normalized = normalized.dropna(subset=["timestamp", "price_per_kwh"])
    normalized = normalized.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)

    if normalized.empty:
        raise CaisoPricingError("CAISO pricing normalization produced no usable price rows.")

    return finalize_normalized_price_frame(normalized)


def _build_caiso_request_params(
    *,
    price_node: str,
    start_time: Any,
    end_time: Any,
    market_run_id: str,
) -> dict[str, str | int]:
    start_ts = pd.to_datetime(start_time, errors="coerce")
    end_ts = pd.to_datetime(end_time, errors="coerce")
    if pd.isna(start_ts) or pd.isna(end_ts):
        raise CaisoPricingError("CAISO pricing requires valid start and end timestamps.")

    request_start = start_ts.floor("h")
    request_end = end_ts.ceil("h") + timedelta(hours=1)
    if request_end <= request_start:
        request_end = request_start + timedelta(hours=1)

    return {
        "queryname": "PRC_LMP",
        "market_run_id": market_run_id,
        "node": price_node,
        "startdatetime": _format_oasis_timestamp(request_start),
        "enddatetime": _format_oasis_timestamp(request_end),
        "resultformat": 6,
        "version": "1",
    }


def fetch_caiso_day_ahead_prices(
    *,
    price_node: str,
    region_code: str = "CAISO",
    start_time: Any,
    end_time: Any,
    market_run_id: str = "DAM",
    timeout_seconds: int = CAISO_DEFAULT_TIMEOUT_SECONDS,
    max_retry_attempts: int = CAISO_DEFAULT_MAX_RETRY_ATTEMPTS,
    retry_sleep_seconds: float = CAISO_DEFAULT_RETRY_SLEEP_SECONDS,
) -> pd.DataFrame:
    """
    Fetch CAISO day-ahead hourly LMP data from OASIS PRC_LMP.

    CAISO documents PRC_LMP as hourly day-ahead locational marginal prices in $/MWh.
    Util normalizes this to $/kWh for internal optimization use.
    """
    params = _build_caiso_request_params(
        price_node=price_node,
        start_time=start_time,
        end_time=end_time,
        market_run_id=market_run_id,
    )
    cache_key = (
        price_node,
        region_code,
        params["startdatetime"],
        params["enddatetime"],
        market_run_id,
    )
    cached_rows = _fetch_caiso_day_ahead_prices_cached(
        cache_key=cache_key,
        timeout_seconds=timeout_seconds,
        max_retry_attempts=max_retry_attempts,
        retry_sleep_seconds=retry_sleep_seconds,
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
def _fetch_caiso_day_ahead_prices_cached(
    *,
    cache_key: tuple[str, str, str, str, str],
    timeout_seconds: int,
    max_retry_attempts: int,
    retry_sleep_seconds: float,
) -> tuple[tuple[pd.Timestamp, str, float, float, str, str, str, float, str, str, bool, str, str, str], ...]:
    price_node, region_code, startdatetime, enddatetime, market_run_id = cache_key
    params = {
        "queryname": "PRC_LMP",
        "market_run_id": market_run_id,
        "node": price_node,
        "startdatetime": startdatetime,
        "enddatetime": enddatetime,
        "resultformat": 6,
        "version": "1",
    }

    response = None
    attempts = max(1, int(max_retry_attempts))
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                CAISO_OASIS_SINGLE_ZIP_URL,
                params=params,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            raise CaisoPricingError(f"CAISO network request failed: {exc}") from exc

        caiso_logger.info(
            "Util CAISO: response status=%s attempt=%s/%s node=%s region=%s url=%s",
            response.status_code,
            attempt,
            attempts,
            price_node,
            region_code,
            response.url,
        )

        if response.status_code not in CAISO_RETRYABLE_STATUS_CODES:
            break

        caiso_logger.warning(
            "Util CAISO: rate limited with status=%s attempt=%s/%s node=%s region=%s",
            response.status_code,
            attempt,
            attempts,
            price_node,
            region_code,
        )

        if attempt < attempts:
            caiso_logger.warning(
                "Util CAISO: retrying after %.1f seconds due to rate limit",
                retry_sleep_seconds,
            )
            time.sleep(min(retry_sleep_seconds, 1.0))

    if response is None:
        raise CaisoPricingError("CAISO pricing request did not return a response.")

    if response.status_code >= 400:
        preview = response.text[:500]
        raise CaisoPricingError(
            f"CAISO request failed with status {response.status_code}. Response preview: {preview}"
        )

    raw_df = _extract_first_csv(response.content)
    normalized_df = _normalize_caiso_lmp_dataframe(
        raw_df,
        price_node=price_node,
        region_code=region_code,
    )
    caiso_logger.info(
        "Util CAISO: parsed pricing rows=%s node=%s region=%s",
        len(normalized_df),
        price_node,
        region_code,
    )
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
