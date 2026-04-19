from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd


# ---------------------------------------------------------------------------
# Output contract
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FeasibilityFeatures:
    """
    All raw derived signals for one feasibility analysis run.

    Every numeric field is ``None`` when the underlying data column is absent
    or entirely null — callers must handle ``None`` gracefully.  String/enum
    fields always have a value.

    Units are explicit in field names wherever they matter.
    """

    # ---- Price signals -------------------------------------------------- #

    avg_price: float | None
    """Mean price_per_kwh across all forecast intervals."""

    peak_price: float | None
    """Max price_per_kwh in the forecast window."""

    price_volatility: float | None
    """Standard deviation of price_per_kwh (absolute, not normalised).
    Use this to measure signal noise; higher = more scheduling opportunity."""

    price_range: float | None
    """peak_price - min(price_per_kwh). Absolute spread in $/kWh."""

    price_spike_share: float | None
    """Fraction of intervals above the 75th-percentile price threshold.
    Defined as top-quartile share; always 0.25 on a uniform distribution."""

    cheap_window_share: float | None
    """Fraction of intervals at or below the 25th-percentile price threshold.
    Identifies how many intervals qualify as low-cost scheduling targets."""

    peak_price_frequency: float | None
    """Fraction of intervals in the top decile (>= 90th percentile) of price.
    Highlights severe price spikes vs general volatility."""

    # ---- Carbon signals ------------------------------------------------- #

    avg_carbon: float | None
    """Mean carbon_g_per_kwh across all forecast intervals."""

    peak_carbon: float | None
    """Max carbon_g_per_kwh in the forecast window."""

    carbon_volatility: float | None
    """Standard deviation of carbon_g_per_kwh.
    Higher = more scheduling opportunity on the carbon dimension."""

    carbon_range: float | None
    """peak_carbon - min(carbon_g_per_kwh). Absolute spread in g CO₂/kWh."""

    high_carbon_share: float | None
    """Fraction of intervals above the 75th-percentile carbon threshold."""

    clean_window_share: float | None
    """Fraction of intervals at or below the 25th-percentile carbon threshold."""

    # ---- Timing signals ------------------------------------------------- #

    hours_until_deadline: float | None
    """Wall-clock hours from the first forecast timestamp to the deadline.
    None when the deadline cannot be parsed or precedes the forecast window."""

    compute_hours_required: int
    """Passed through from the workload input unchanged."""

    runtime_density: float | None
    """compute_hours_required / hours_until_deadline.
    0.0 = trivially schedulable; >= 1.0 = infeasible (not enough window)."""

    deadline_tightness: float | None
    """required_intervals / available_intervals.
    Equivalent to runtime_density when intervals are 1 hour each.
    Capped at 1.0 in practice; > 1.0 signals an infeasible workload."""

    available_favorable_window_hours: int | None
    """Count of intervals that are simultaneously cheap (<= P25 price) AND
    clean (<= P25 carbon).  Each interval represents one scheduling hour."""

    favorable_window_coverage: float | None
    """available_favorable_window_hours / compute_hours_required.
    >= 1.0 = enough favorable slots exist; < 1.0 = constrained choice."""

    timing_mismatch_score: float | None
    """Normalised position (0.0–1.0) of favorable windows within the deadline
    window.  0.0 = all favorable slots are early (schedule soon); 1.0 = all
    favorable slots are late (must wait until near the deadline)."""

    # ---- Load signals --------------------------------------------------- #

    machine_kw: float
    """machine_watts / 1000.  Power draw in kilowatts."""

    load_energy_required_kwh: float
    """machine_kw * compute_hours_required.  Total energy needed for the run."""

    load_pressure: float | None
    """load_energy_required_kwh * avg_price.  Estimated baseline cost in USD
    at average grid price.  Not an optimised forecast — a rough magnitude check."""

    relative_load_bucket: Literal["light", "medium", "heavy"]
    """Categorical label based on deadline_tightness:
      light  — deadline_tightness < 0.25 (lots of room to schedule)
      medium — 0.25 <= deadline_tightness < 0.65
      heavy  — deadline_tightness >= 0.65 OR infeasible"""

    # ---- Combined proxy signals ----------------------------------------- #

    grid_stress_proxy: float | None
    """Mean of high_carbon_share and price_spike_share.
    0.0 = clean cheap grid; 1.0 = every interval is both dirty and expensive."""

    market_instability_proxy: float | None
    """Mean of the coefficients of variation for price and carbon signals:
      (price_std/price_mean + carbon_std/carbon_mean) / 2
    Higher = more volatile market; also means more scheduling opportunity."""

    urgency_stress_proxy: float | None
    """deadline_tightness * (1 - cheap_window_share).
    Peaks when the deadline is tight AND cheap windows are rare."""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def extract_feasibility_features(
    forecast_df: pd.DataFrame,
    compute_hours_required: int,
    deadline: str,
    machine_watts: int,
) -> FeasibilityFeatures:
    """
    Derive all FeasibilityFeatures from the pipeline forecast DataFrame.

    Parameters
    ----------
    forecast_df:
        DataFrame produced by ``get_forecast_table()``.  Expected columns:
        ``timestamp``, ``price_per_kwh``, ``carbon_g_per_kwh``.
        Any subset of these may be null or missing.
    compute_hours_required:
        Workload size in whole hours.  Each hour maps to one forecast interval
        because the pipeline uses 1-hour intervals throughout.
    deadline:
        ISO-8601 deadline string.  Timezone-naive strings are treated as local
        (America/Los_Angeles) to match ``data_fetcher.py`` normalisation.
    machine_watts:
        Machine power draw in watts.  Used only for load calculations;
        does not affect scheduling signals.

    Returns
    -------
    FeasibilityFeatures
        All fields are populated.  Numeric fields are ``None`` when data is
        unavailable; the function never raises on bad/missing data.
    """
    price = _safe_series(forecast_df, "price_per_kwh")
    carbon = _safe_series(forecast_df, "carbon_g_per_kwh")
    timestamps = _safe_series(forecast_df, "timestamp")

    # ---- Price ---------------------------------------------------------- #
    avg_price = _safe_mean(price)
    peak_price = _safe_max(price)
    price_volatility = _safe_std(price)
    price_range = _safe_range(price)
    price_spike_share = _above_percentile_share(price, 75)
    cheap_window_share = _at_or_below_percentile_share(price, 25)
    peak_price_frequency = _above_percentile_share(price, 90)

    # ---- Carbon --------------------------------------------------------- #
    avg_carbon = _safe_mean(carbon)
    peak_carbon = _safe_max(carbon)
    carbon_volatility = _safe_std(carbon)
    carbon_range = _safe_range(carbon)
    high_carbon_share = _above_percentile_share(carbon, 75)
    clean_window_share = _at_or_below_percentile_share(carbon, 25)

    # ---- Timing --------------------------------------------------------- #
    deadline_ts = _parse_deadline(deadline)
    first_ts = _first_timestamp(timestamps)

    hours_until_deadline = _hours_between(first_ts, deadline_ts)

    # Count how many intervals fall at or before the deadline.
    available_intervals = _available_interval_count(timestamps, deadline_ts)
    required_intervals = compute_hours_required

    runtime_density = _safe_divide(required_intervals, hours_until_deadline)

    # deadline_tightness uses interval counts (more accurate than wall-clock
    # hours when the forecast has gaps).
    deadline_tightness = _safe_divide(required_intervals, available_intervals)

    avail_favorable = _favorable_interval_count(price, carbon, timestamps, deadline_ts)
    favorable_window_coverage = _safe_divide(avail_favorable, required_intervals)

    timing_mismatch = _timing_mismatch_score(price, carbon, timestamps, deadline_ts)

    # ---- Load ----------------------------------------------------------- #
    machine_kw = machine_watts / 1000.0
    load_energy_required_kwh = machine_kw * compute_hours_required
    load_pressure = (
        round(load_energy_required_kwh * avg_price, 6)
        if avg_price is not None
        else None
    )
    relative_load_bucket = _load_bucket(deadline_tightness)

    # ---- Combined proxies ----------------------------------------------- #
    grid_stress_proxy = _grid_stress(high_carbon_share, price_spike_share)
    market_instability_proxy = _market_instability(price, carbon)
    urgency_stress_proxy = _urgency_stress(deadline_tightness, cheap_window_share)

    return FeasibilityFeatures(
        avg_price=avg_price,
        peak_price=peak_price,
        price_volatility=price_volatility,
        price_range=price_range,
        price_spike_share=price_spike_share,
        cheap_window_share=cheap_window_share,
        peak_price_frequency=peak_price_frequency,
        avg_carbon=avg_carbon,
        peak_carbon=peak_carbon,
        carbon_volatility=carbon_volatility,
        carbon_range=carbon_range,
        high_carbon_share=high_carbon_share,
        clean_window_share=clean_window_share,
        hours_until_deadline=hours_until_deadline,
        compute_hours_required=compute_hours_required,
        runtime_density=runtime_density,
        deadline_tightness=deadline_tightness,
        available_favorable_window_hours=avail_favorable,
        favorable_window_coverage=favorable_window_coverage,
        timing_mismatch_score=timing_mismatch,
        machine_kw=machine_kw,
        load_energy_required_kwh=load_energy_required_kwh,
        load_pressure=load_pressure,
        relative_load_bucket=relative_load_bucket,
        grid_stress_proxy=grid_stress_proxy,
        market_instability_proxy=market_instability_proxy,
        urgency_stress_proxy=urgency_stress_proxy,
    )


# ---------------------------------------------------------------------------
# Internal helpers — all return None on bad input, never raise
# ---------------------------------------------------------------------------

def _safe_series(df: pd.DataFrame, col: str) -> pd.Series | None:
    """Return a non-null numeric (or datetime) series, or None if unavailable."""
    if col not in df.columns:
        return None
    s = df[col].dropna()
    if s.empty:
        return None
    return s


def _safe_mean(s: pd.Series | None) -> float | None:
    if s is None:
        return None
    v = pd.to_numeric(s, errors="coerce").dropna()
    return float(v.mean()) if not v.empty else None


def _safe_max(s: pd.Series | None) -> float | None:
    if s is None:
        return None
    v = pd.to_numeric(s, errors="coerce").dropna()
    return float(v.max()) if not v.empty else None


def _safe_std(s: pd.Series | None) -> float | None:
    """Population std (ddof=0) so single-interval forecasts return 0 not NaN."""
    if s is None:
        return None
    v = pd.to_numeric(s, errors="coerce").dropna()
    return float(v.std(ddof=0)) if not v.empty else None


def _safe_range(s: pd.Series | None) -> float | None:
    if s is None:
        return None
    v = pd.to_numeric(s, errors="coerce").dropna()
    return float(v.max() - v.min()) if not v.empty else None


def _safe_divide(numerator: float | int | None, denominator: float | int | None) -> float | None:
    """Return numerator / denominator, or None on division-by-zero / None input."""
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return float(numerator) / float(denominator)


def _above_percentile_share(s: pd.Series | None, pct: float) -> float | None:
    """
    Fraction of values strictly above the ``pct``-th percentile of the series.
    Uses the series itself as the reference distribution (no external baseline).
    Returns None when the series is None or has fewer than 2 values.
    """
    if s is None:
        return None
    v = pd.to_numeric(s, errors="coerce").dropna()
    if len(v) < 2:
        return None
    threshold = float(v.quantile(pct / 100.0))
    return float((v > threshold).mean())


def _at_or_below_percentile_share(s: pd.Series | None, pct: float) -> float | None:
    """
    Fraction of values at or below the ``pct``-th percentile of the series.
    Complement of ``_above_percentile_share`` at the same boundary.
    """
    if s is None:
        return None
    v = pd.to_numeric(s, errors="coerce").dropna()
    if len(v) < 2:
        return None
    threshold = float(v.quantile(pct / 100.0))
    return float((v <= threshold).mean())


def _parse_deadline(deadline: str) -> pd.Timestamp | None:
    """
    Parse the deadline string into a timezone-naive local timestamp.
    Matches the normalisation in ``data_fetcher._normalize_timestamp_column``
    (America/Los_Angeles, tz-naive for arithmetic).
    Returns None on parse failure.
    """
    try:
        ts = pd.to_datetime(deadline)
        if ts.tzinfo is not None:
            ts = ts.tz_convert("America/Los_Angeles").tz_localize(None)
        return ts
    except Exception:
        return None


def _first_timestamp(timestamps: pd.Series | None) -> pd.Timestamp | None:
    """Return the earliest timestamp in the series, or None."""
    if timestamps is None:
        return None
    try:
        parsed = pd.to_datetime(timestamps, errors="coerce").dropna().sort_values()
        if parsed.empty:
            return None
        ts = parsed.iloc[0]
        if getattr(ts, "tzinfo", None) is not None:
            ts = ts.tz_convert("America/Los_Angeles").tz_localize(None)
        return ts
    except Exception:
        return None


def _hours_between(start: pd.Timestamp | None, end: pd.Timestamp | None) -> float | None:
    """Wall-clock hours from start to end.  Returns None if either is None or end <= start."""
    if start is None or end is None:
        return None
    delta = end - start
    hours = delta.total_seconds() / 3600.0
    return hours if hours > 0 else None


def _available_interval_count(
    timestamps: pd.Series | None,
    deadline: pd.Timestamp | None,
) -> int:
    """
    Count forecast intervals whose timestamp is at or before the deadline.
    Returns 0 when timestamps or deadline are unavailable.
    """
    if timestamps is None or deadline is None:
        return 0
    try:
        parsed = pd.to_datetime(timestamps, errors="coerce").dropna()
        # Strip timezone for comparison (data_fetcher normalises to tz-naive local).
        def _strip_tz(ts_series: pd.Series) -> pd.Series:
            if hasattr(ts_series.dt, "tz") and ts_series.dt.tz is not None:
                return ts_series.dt.tz_convert("America/Los_Angeles").dt.tz_localize(None)
            return ts_series

        parsed = _strip_tz(parsed)
        return int((parsed <= deadline).sum())
    except Exception:
        return 0


def _favorable_interval_count(
    price: pd.Series | None,
    carbon: pd.Series | None,
    timestamps: pd.Series | None,
    deadline: pd.Timestamp | None,
) -> int | None:
    """
    Count intervals within the deadline window that are simultaneously
    cheap (<= P25 price) AND clean (<= P25 carbon).

    Returns None when both price and carbon data are missing.
    Falls back to single-dimension filtering when only one signal is available.
    """
    if price is None and carbon is None:
        return None

    # Build a working DataFrame aligned on the original index.
    parts: dict[str, pd.Series] = {}
    if price is not None:
        parts["price"] = pd.to_numeric(price, errors="coerce")
    if carbon is not None:
        parts["carbon"] = pd.to_numeric(carbon, errors="coerce")

    df = pd.DataFrame(parts)

    # Apply deadline mask using timestamps if available.
    if timestamps is not None and deadline is not None:
        try:
            ts = pd.to_datetime(timestamps, errors="coerce")
            if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
                ts = ts.dt.tz_convert("America/Los_Angeles").dt.tz_localize(None)
            df = df.loc[ts.values <= deadline]
        except Exception:
            pass  # proceed without deadline filtering

    if df.empty:
        return 0

    mask = pd.Series([True] * len(df), index=df.index)

    if "price" in df.columns:
        p = df["price"].dropna()
        if len(p) >= 2:
            p_threshold = float(p.quantile(0.25))
            mask &= df["price"].fillna(float("inf")) <= p_threshold

    if "carbon" in df.columns:
        c = df["carbon"].dropna()
        if len(c) >= 2:
            c_threshold = float(c.quantile(0.25))
            mask &= df["carbon"].fillna(float("inf")) <= c_threshold

    return int(mask.sum())


def _timing_mismatch_score(
    price: pd.Series | None,
    carbon: pd.Series | None,
    timestamps: pd.Series | None,
    deadline: pd.Timestamp | None,
) -> float | None:
    """
    Normalised mean position (0.0–1.0) of favorable intervals within the window.

    Formula:
      - Build a position index over all timestamps within the deadline
        (0.0 = first interval, 1.0 = last interval).
      - Identify favorable intervals: cheap (<= P25 price) AND/OR clean (<= P25 carbon).
      - Return the mean position of favorable intervals.

    Interpretation:
      0.0 = all favorable slots are early — schedule soon, low friction.
      1.0 = all favorable slots are near the deadline — must wait, higher friction.
      None = cannot compute (no timestamps or no favorable windows found).
    """
    if timestamps is None:
        return None

    # Build position frame: index = original DataFrame index, column = 0.0–1.0 position.
    pos_frame = _build_ts_frame(timestamps, deadline)
    if len(pos_frame) < 2:
        return None

    fav_mask = pd.Series(True, index=pos_frame.index)

    if price is not None:
        p = pd.to_numeric(price, errors="coerce").reindex(pos_frame.index)
        p_valid = p.dropna()
        if len(p_valid) >= 2:
            fav_mask &= p.fillna(float("inf")) <= float(p_valid.quantile(0.25))

    if carbon is not None:
        c = pd.to_numeric(carbon, errors="coerce").reindex(pos_frame.index)
        c_valid = c.dropna()
        if len(c_valid) >= 2:
            fav_mask &= c.fillna(float("inf")) <= float(c_valid.quantile(0.25))

    fav_positions = pos_frame.loc[fav_mask, "position"]
    if fav_positions.empty:
        return None

    return float(fav_positions.mean())


def _build_ts_frame(
    timestamps: pd.Series | None,
    deadline: pd.Timestamp | None,
) -> pd.DataFrame:
    """
    Build a sorted DataFrame of (original_index, position) for timestamps
    within the deadline window.  position is normalised 0.0–1.0.
    """
    if timestamps is None:
        return pd.DataFrame(columns=["position"])

    try:
        ts = pd.to_datetime(timestamps, errors="coerce")
        if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
            ts = ts.dt.tz_convert("America/Los_Angeles").dt.tz_localize(None)

        df = pd.DataFrame({"ts": ts, "original_index": timestamps.index})
        df = df.dropna(subset=["ts"]).sort_values("ts").reset_index(drop=True)

        if deadline is not None:
            df = df[df["ts"] <= deadline].reset_index(drop=True)

        n = len(df)
        if n == 0:
            return pd.DataFrame(columns=["position"])

        df["position"] = [i / max(n - 1, 1) for i in range(n)]
        df = df.set_index("original_index")
        return df
    except Exception:
        return pd.DataFrame(columns=["position"])


def _load_bucket(
    deadline_tightness: float | None,
) -> Literal["light", "medium", "heavy"]:
    """
    Classify scheduling pressure from deadline_tightness:
      light  — < 0.25  (workload fits into < 25% of the available window)
      medium — 0.25–0.65
      heavy  — >= 0.65 or infeasible (tightness None treated as heavy)
    """
    if deadline_tightness is None or deadline_tightness >= 0.65:
        return "heavy"
    if deadline_tightness >= 0.25:
        return "medium"
    return "light"


def _grid_stress(
    high_carbon_share: float | None,
    price_spike_share: float | None,
) -> float | None:
    """
    (high_carbon_share + price_spike_share) / 2.
    Returns the available single-dimension value if only one is present.
    """
    values = [v for v in (high_carbon_share, price_spike_share) if v is not None]
    if not values:
        return None
    return float(sum(values) / len(values))


def _market_instability(
    price: pd.Series | None,
    carbon: pd.Series | None,
) -> float | None:
    """
    Mean of coefficient-of-variation for price and carbon.
    CV = std / mean; undefined when mean == 0.
    Returns the single available CV when only one signal is present.
    """
    cvs: list[float] = []
    for s in (price, carbon):
        if s is None:
            continue
        v = pd.to_numeric(s, errors="coerce").dropna()
        if v.empty:
            continue
        mean = float(v.mean())
        if mean == 0:
            continue
        cvs.append(float(v.std(ddof=0)) / mean)
    if not cvs:
        return None
    return float(sum(cvs) / len(cvs))


def _urgency_stress(
    deadline_tightness: float | None,
    cheap_window_share: float | None,
) -> float | None:
    """
    deadline_tightness * (1 - cheap_window_share).
    High when the deadline is tight AND cheap windows are rare.
    Returns None when either component is unavailable.
    """
    if deadline_tightness is None or cheap_window_share is None:
        return None
    return float(deadline_tightness) * (1.0 - float(cheap_window_share))
