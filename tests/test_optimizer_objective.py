from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.optimizer import optimize_schedule


def test_objective_cost_picks_lowest_price_intervals() -> None:
    forecast_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-03-21 00:00:00", periods=4, freq="h"),
            "carbon_g_per_kwh": [100, 400, 300, 200],
            "price_per_kwh": [0.50, 0.10, 0.15, 0.40],
        }
    )

    result = optimize_schedule(
        forecast_df=forecast_df,
        compute_hours_required=2,
        objective="cost",
        schedule_mode="flexible",
        current_time_override="2026-03-20 00:00:00",
    )

    selected = result[result["run_flag"] == 1].sort_values("timestamp")
    assert selected["price_per_kwh"].tolist() == [0.10, 0.15]


def test_objective_carbon_picks_lowest_carbon_intervals() -> None:
    forecast_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-03-21 00:00:00", periods=4, freq="h"),
            "carbon_g_per_kwh": [100, 400, 300, 200],
            "price_per_kwh": [0.50, 0.10, 0.15, 0.40],
        }
    )

    result = optimize_schedule(
        forecast_df=forecast_df,
        compute_hours_required=2,
        objective="carbon",
        schedule_mode="flexible",
        current_time_override="2026-03-20 00:00:00",
    )

    selected = result[result["run_flag"] == 1].sort_values("timestamp")
    assert selected["carbon_g_per_kwh"].tolist() == [100, 200]
