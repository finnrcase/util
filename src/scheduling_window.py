from __future__ import annotations

import math

import pandas as pd


APP_TIMEZONE = "America/Los_Angeles"
INFEASIBLE_WORKLOAD_MESSAGE = (
    "The requested compute hours do not fit between the current time and the deadline. "
    "Please reduce compute hours or extend the deadline."
)


class InfeasibleScheduleError(ValueError):
    """Raised when the requested workload cannot fit inside the feasible window."""


def normalize_local_timestamp(value) -> pd.Timestamp:
    ts = pd.to_datetime(value)
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_convert(APP_TIMEZONE).tz_localize(None)
    return ts


def get_current_reference_time(current_time_override: str | None = None) -> pd.Timestamp:
    if current_time_override is not None:
        return normalize_local_timestamp(current_time_override)
    return pd.Timestamp.now(tz=APP_TIMEZONE).tz_localize(None)


def calculate_required_slots(compute_hours_required: int, interval_minutes: float) -> int:
    return math.ceil((compute_hours_required * 60) / interval_minutes)


def build_eligibility_mask(
    timestamps: pd.Series,
    deadline=None,
    current_time_override: str | None = None,
) -> tuple[pd.Series, pd.Timestamp, pd.Timestamp | None]:
    now_ts = get_current_reference_time(current_time_override)

    if timestamps.max() < now_ts:
        effective_now_ts = timestamps.min()
    else:
        effective_now_ts = now_ts

    eligible_mask = timestamps >= effective_now_ts
    deadline_ts = None

    if deadline is not None:
        deadline_ts = normalize_local_timestamp(deadline)
        eligible_mask = eligible_mask & (timestamps <= deadline_ts)

    return eligible_mask, effective_now_ts, deadline_ts


def ensure_window_feasibility(
    slots_required: int,
    eligible_count: int,
):
    if slots_required > eligible_count:
        raise InfeasibleScheduleError(INFEASIBLE_WORKLOAD_MESSAGE)
