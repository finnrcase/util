from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.optimizer import optimize_schedule
from src.scheduling_window import InfeasibleScheduleError


def test_block_mode_selects_single_best_contiguous_window():
    timestamps = pd.date_range("2026-03-20 00:00:00", periods=12, freq="h")
    forecast_df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "carbon_g_per_kwh": [50, 45, 10, 9, 8, 7, 40, 42, 44, 46, 48, 50],
            "price_per_kwh": [0.20] * 12,
        }
    )

    optimized_df = optimize_schedule(
        forecast_df=forecast_df,
        compute_hours_required=4,
        objective="carbon",
        schedule_mode="block",
        current_time_override="2026-03-20 00:00:00",
    )

    selected_rows = optimized_df[optimized_df["run_flag"] == 1].copy()

    assert len(selected_rows) == 4
    assert selected_rows["timestamp"].tolist() == list(
        pd.date_range("2026-03-20 02:00:00", periods=4, freq="h")
    )


def test_block_mode_fails_when_no_contiguous_window_is_available():
    timestamps = pd.to_datetime(
        [
            "2026-03-20 00:00:00",
            "2026-03-20 01:00:00",
            "2026-03-20 03:00:00",
            "2026-03-20 04:00:00",
        ]
    )
    forecast_df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "carbon_g_per_kwh": [10, 9, 8, 7],
            "price_per_kwh": [0.20, 0.20, 0.20, 0.20],
        }
    )

    with pytest.raises(InfeasibleScheduleError):
        optimize_schedule(
            forecast_df=forecast_df,
            compute_hours_required=3,
            objective="carbon",
            schedule_mode="block",
            current_time_override="2026-03-20 00:00:00",
        )
