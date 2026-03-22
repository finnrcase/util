from pathlib import Path
import sys

import pandas as pd
import pytest

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


def test_objective_balanced_uses_weighted_normalized_score() -> None:
    forecast_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-03-21 00:00:00", periods=4, freq="h"),
            "carbon_g_per_kwh": [0, 10, 100, 100],
            "price_per_kwh": [1.00, 0.00, 0.20, 0.10],
        }
    )

    result = optimize_schedule(
        forecast_df=forecast_df,
        compute_hours_required=1,
        objective="balanced",
        carbon_weight=0.5,
        price_weight=0.5,
        schedule_mode="flexible",
        current_time_override="2026-03-20 00:00:00",
    )

    selected = result[result["run_flag"] == 1].sort_values("timestamp")
    assert selected["timestamp"].tolist() == [pd.Timestamp("2026-03-21 01:00:00")]


def test_balanced_handles_flat_metric_without_breaking_score() -> None:
    forecast_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-03-21 00:00:00", periods=4, freq="h"),
            "carbon_g_per_kwh": [100, 100, 100, 100],
            "price_per_kwh": [0.40, 0.10, 0.20, 0.30],
        }
    )

    result = optimize_schedule(
        forecast_df=forecast_df,
        compute_hours_required=2,
        objective="balanced",
        carbon_weight=0.5,
        price_weight=0.5,
        schedule_mode="flexible",
        current_time_override="2026-03-20 00:00:00",
    )

    selected = result[result["run_flag"] == 1].sort_values("timestamp")
    assert selected["price_per_kwh"].tolist() == [0.10, 0.20]
    assert (result["score"].dropna() >= 0).all()


def test_balanced_fails_cleanly_when_price_data_is_missing() -> None:
    forecast_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-03-21 00:00:00", periods=3, freq="h"),
            "carbon_g_per_kwh": [100, 200, 300],
            "price_per_kwh": [0.10, None, 0.30],
        }
    )

    with pytest.raises(
        ValueError,
        match="Balanced optimization requires both carbon and price data",
    ):
        optimize_schedule(
            forecast_df=forecast_df,
            compute_hours_required=1,
            objective="balanced",
            carbon_weight=0.5,
            price_weight=0.5,
            schedule_mode="flexible",
            current_time_override="2026-03-20 00:00:00",
        )
