from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache
import html
import re
from typing import Any

import pandas as pd
import requests

from src.price_adapters.base import PriceProviderError, finalize_normalized_price_frame
from src.scheduling_window import APP_TIMEZONE


ERCOT_DAM_SPP_URL_TEMPLATE = "https://www.ercot.com/content/cdr/html/{date_key}_dam_spp.html"
ERCOT_PRICE_SOURCE_LABEL = "ERCOT"
ERCOT_MARKET_LABEL = "DAM"
ERCOT_TIMEZONE = "America/Chicago"


class ErcotPricingError(PriceProviderError):
    """Raised when ERCOT pricing data cannot be fetched or normalized."""


def _coerce_to_ercot_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        raise ErcotPricingError(f"Invalid ERCOT timestamp boundary: {value}")

    if getattr(ts, "tzinfo", None) is None:
        ts = ts.tz_localize(APP_TIMEZONE)
    else:
        ts = ts.tz_convert(APP_TIMEZONE)

    return ts.tz_convert(ERCOT_TIMEZONE)


def _iter_delivery_dates(start_time: Any, end_time: Any) -> list[date]:
    start_ts = _coerce_to_ercot_timestamp(start_time)
    end_ts = _coerce_to_ercot_timestamp(end_time)

    # ERCOT DAM pages are keyed by operating day. We only need the operating days
    # that contain the hourly interval starts used for backward price alignment,
    # not the next whole calendar day after an arbitrary minute/second endpoint.
    start_day = start_ts.floor("h").date()
    end_day = end_ts.floor("h").date()
    if end_day < start_day:
        end_day = start_day

    delivery_dates: list[date] = []
    cursor = start_day
    while cursor <= end_day:
        delivery_dates.append(cursor)
        cursor += timedelta(days=1)

    return delivery_dates


def _fetch_ercot_dam_table(delivery_date: date) -> pd.DataFrame:
    date_key = delivery_date.strftime("%Y%m%d")
    cached_rows = _fetch_ercot_dam_table_cached(date_key)
    return pd.DataFrame(cached_rows)


@lru_cache(maxsize=64)
def _fetch_ercot_dam_table_cached(date_key: str) -> tuple[dict[str, object], ...]:
    url = ERCOT_DAM_SPP_URL_TEMPLATE.format(date_key=date_key)

    try:
        response = requests.get(url, timeout=45)
    except requests.RequestException as exc:
        raise ErcotPricingError(f"ERCOT network request failed for {date_key}: {exc}") from exc

    print(f"[ERCOT DEBUG] Request URL: {response.url}")
    print(f"[ERCOT DEBUG] Response status: {response.status_code}")

    if response.status_code >= 400:
        raise ErcotPricingError(
            f"ERCOT DAM settlement point price page request failed with status {response.status_code} for {date_key}."
        )

    matching_table = _extract_matching_table(response.text)
    if matching_table is None:
        raise ErcotPricingError(
            f"Could not find the ERCOT DAM settlement point table for {date_key}."
        )
    return tuple(matching_table.to_dict(orient="records"))


def _is_soft_unavailable_delivery_date_error(exc: ErcotPricingError) -> bool:
    message = str(exc)
    return (
        "Could not find the ERCOT DAM settlement point table" in message
        or "status 404" in message
    )


def _strip_html(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    return html.unescape(re.sub(r"\s+", " ", without_tags)).strip()


def _extract_matching_table(html_text: str) -> pd.DataFrame | None:
    table_matches = re.findall(r"<table.*?>.*?</table>", html_text, flags=re.IGNORECASE | re.DOTALL)
    for table_html in table_matches:
        row_matches = re.findall(r"<tr.*?>.*?</tr>", table_html, flags=re.IGNORECASE | re.DOTALL)
        parsed_rows: list[list[str]] = []
        for row_html in row_matches:
            cell_matches = re.findall(r"<t[hd].*?>(.*?)</t[hd]>", row_html, flags=re.IGNORECASE | re.DOTALL)
            if cell_matches:
                parsed_rows.append([_strip_html(cell) for cell in cell_matches])

        if len(parsed_rows) < 2:
            continue

        header = parsed_rows[0]
        if "Oper Day" not in header or "Hour Ending" not in header:
            continue

        body_rows = [row for row in parsed_rows[1:] if any(cell for cell in row)]
        if not body_rows:
            continue

        width = len(header)
        normalized_rows = []
        for row in body_rows:
            padded = list(row[:width]) + [""] * max(width - len(row), 0)
            normalized_rows.append(padded[:width])

        return pd.DataFrame(normalized_rows, columns=header)

    return None


def _normalize_hour_ending(value: Any) -> int:
    text = str(value).strip()
    digits = "".join(character for character in text if character.isdigit())
    if not digits:
        raise ErcotPricingError(f"Could not parse ERCOT Hour Ending value '{value}'.")

    hour_ending = int(digits)
    if hour_ending < 1 or hour_ending > 24:
        raise ErcotPricingError(f"ERCOT Hour Ending value '{value}' is outside the expected 1-24 range.")

    return hour_ending


def _normalize_ercot_day_table(
    day_df: pd.DataFrame,
    *,
    settlement_point: str,
    region_code: str,
) -> pd.DataFrame:
    if settlement_point not in day_df.columns:
        available_points = [column for column in day_df.columns if column not in {"Oper Day", "Hour Ending"}]
        raise ErcotPricingError(
            f"ERCOT DAM table did not include settlement point '{settlement_point}'. "
            f"Available columns: {available_points[:20]}"
        )

    oper_day_series = pd.to_datetime(day_df["Oper Day"], errors="coerce")
    hour_ending_series = day_df["Hour Ending"].map(_normalize_hour_ending)
    price_series = pd.to_numeric(day_df[settlement_point], errors="coerce")

    timestamps = []
    for oper_day, hour_ending in zip(oper_day_series, hour_ending_series, strict=False):
        if pd.isna(oper_day):
            timestamps.append(pd.NaT)
            continue

        interval_start = pd.Timestamp(oper_day.date()) + pd.Timedelta(hours=hour_ending - 1)
        localized = interval_start.tz_localize(ERCOT_TIMEZONE)
        timestamps.append(localized.tz_convert(APP_TIMEZONE).tz_localize(None))

    normalized = pd.DataFrame(
        {
            "timestamp": timestamps,
            "local_time": pd.to_datetime(timestamps, errors="coerce").strftime("%Y-%m-%d %H:%M:%S"),
            "price_per_mwh": price_series,
            "price_per_kwh": price_series / 1000.0,
            "source_market": ERCOT_MARKET_LABEL,
            "source_provider": ERCOT_PRICE_SOURCE_LABEL,
            "node_or_zone": settlement_point,
            "interval_minutes": 60.0,
            "price_type": "day_ahead_settlement_point_price",
            "is_forecast_or_historical": "forecast",
            "is_live_market_data": True,
            "source": ERCOT_PRICE_SOURCE_LABEL,
            "region_code": region_code,
            "price_node": settlement_point,
        }
    )
    normalized = normalized.dropna(subset=["timestamp", "price_per_kwh"])
    normalized = normalized.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)

    if normalized.empty:
        raise ErcotPricingError(
            f"ERCOT normalization produced no usable rows for settlement point '{settlement_point}'."
        )

    return finalize_normalized_price_frame(normalized)


def fetch_ercot_prices(
    *,
    region_code: str,
    node_or_zone: str,
    start_time: Any,
    end_time: Any,
    market: str,
) -> pd.DataFrame:
    if str(market).strip().upper() not in {"DAM", "DAY_AHEAD"}:
        raise ErcotPricingError(
            f"ERCOT provider currently supports day-ahead pricing only. Received market '{market}'."
        )

    if str(node_or_zone).strip().upper() == "ERCOT_DEFAULT_ZONE":
        raise ErcotPricingError(
            "ERCOT routing reached the generic placeholder zone 'ERCOT_DEFAULT_ZONE'. "
            "Add an explicit region-to-load-zone mapping before live ERCOT pricing can be used for this region."
        )

    delivery_dates = _iter_delivery_dates(start_time, end_time)
    print(
        "[ERCOT DEBUG] Route metadata:",
        {
            "region_code": region_code,
            "settlement_point": node_or_zone,
            "market": ERCOT_MARKET_LABEL,
            "requested_delivery_dates": [value.isoformat() for value in delivery_dates],
        },
    )

    normalized_frames: list[pd.DataFrame] = []
    loaded_delivery_dates: list[str] = []
    skipped_delivery_dates: list[str] = []
    last_error: ErcotPricingError | None = None
    for delivery_date in delivery_dates:
        date_label = delivery_date.isoformat()
        try:
            day_df = _fetch_ercot_dam_table(delivery_date)
            normalized_frames.append(
                _normalize_ercot_day_table(
                    day_df,
                    settlement_point=node_or_zone,
                    region_code=region_code,
                )
            )
            loaded_delivery_dates.append(date_label)
        except ErcotPricingError as exc:
            last_error = exc
            if _is_soft_unavailable_delivery_date_error(exc):
                skipped_delivery_dates.append(date_label)
                print(
                    "[ERCOT DEBUG] Skipping unavailable delivery date while preserving earlier live rows:",
                    {
                        "delivery_date": date_label,
                        "reason": str(exc),
                    },
                )
                continue
            raise

    if not normalized_frames:
        if last_error is not None:
            raise last_error
        raise ErcotPricingError("ERCOT pricing fetch returned no usable delivery dates.")

    print(
        "[ERCOT DEBUG] Delivery date fetch summary:",
        {
            "loaded_delivery_dates": loaded_delivery_dates,
            "skipped_delivery_dates": skipped_delivery_dates,
        },
    )

    combined = pd.concat(normalized_frames, ignore_index=True)
    combined = combined.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)

    start_bound = _coerce_to_ercot_timestamp(start_time).tz_convert(APP_TIMEZONE).tz_localize(None)
    end_bound = _coerce_to_ercot_timestamp(end_time).tz_convert(APP_TIMEZONE).tz_localize(None)
    filtered = combined.loc[
        (combined["timestamp"] >= start_bound.floor("h"))
        & (combined["timestamp"] <= (end_bound.ceil("h") + pd.Timedelta(hours=1)))
    ].copy()
    if filtered.empty:
        filtered = combined.copy()

    print(f"[ERCOT DEBUG] Parsed rows: {len(filtered)}")
    print(f"[ERCOT DEBUG] Output sample:\n{filtered.head(3).to_string(index=False)}")
    return finalize_normalized_price_frame(filtered)
