from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.data_fetcher import build_live_carbon_forecast_table, build_live_price_forecast_table
from src.forecasting.carbon_blender import extend_forecast_with_history
from src.forecasting.pattern_extension import _learn_component_weights, build_time_of_day_profile


def test_carbon_extension_uses_historical_pattern_label() -> None:
    live_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-03-21 00:00:00", periods=3, freq="h"),
            "carbon_g_per_kwh": [100, 120, 140],
        }
    )
    historical_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-03-19 03:00:00",
                    "2026-03-20 03:00:00",
                    "2026-03-19 04:00:00",
                    "2026-03-20 04:00:00",
                ]
            ),
            "carbon_g_per_kwh": [50, 50, 60, 60],
        }
    )

    extended_df = extend_forecast_with_history(
        live_forecast_df=live_df,
        historical_df=historical_df,
        deadline="2026-03-21 04:00:00",
    )

    extended_rows = extended_df[extended_df["carbon_source"] == "historical_pattern_estimate"]
    assert not extended_rows.empty
    assert extended_rows["carbon_g_per_kwh"].tolist()[:2] == [50, 60]


def test_hybrid_profile_blends_recent_same_weekday_and_baseline() -> None:
    historical_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-03-02 03:00:00",  # Monday
                    "2026-03-09 03:00:00",  # Monday
                    "2026-03-16 03:00:00",  # Monday
                    "2026-03-17 03:00:00",  # Tuesday
                    "2026-03-18 03:00:00",  # Wednesday
                ]
            ),
            "carbon_g_per_kwh": [40, 80, 100, 20, 60],
        }
    )

    profile_df = build_time_of_day_profile(
        historical_df,
        value_column="carbon_g_per_kwh",
        profile_value_column="historical_avg_carbon_g_per_kwh",
    )

    monday_row = profile_df[
        (profile_df["time_key"] == "03:00") & (profile_df["weekday"] == 0)
    ].iloc[0]

    # recent uses the most recent few days at this time slot: (100 + 20 + 60) / 3 = 60
    # same-weekday mean = (40 + 80 + 100) / 3 = 73.333..., baseline = 60
    expected_value = (0.45 * 60.0) + (0.35 * ((40 + 80 + 100) / 3)) + (0.20 * 60.0)
    assert monday_row["historical_avg_carbon_g_per_kwh"] == pytest.approx(expected_value)


def test_hybrid_profile_renormalizes_when_same_weekday_is_missing() -> None:
    historical_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-03-18 03:00:00",
                    "2026-03-19 03:00:00",
                    "2026-03-20 03:00:00",
                ]
            ),
            "carbon_g_per_kwh": [30, 60, 90],
        }
    )

    extended_df = extend_forecast_with_history(
        live_forecast_df=pd.DataFrame(
            {
                "timestamp": pd.date_range("2026-03-21 00:00:00", periods=3, freq="h"),
                "carbon_g_per_kwh": [100, 120, 140],
            }
        ),
        historical_df=historical_df,
        deadline="2026-03-22 03:00:00",
    )

    sunday_extension = extended_df[
        (extended_df["timestamp"] == pd.Timestamp("2026-03-22 03:00:00"))
        & (extended_df["carbon_source"] == "historical_pattern_estimate")
    ].iloc[0]

    # recent and baseline are both available; same-weekday is missing so weights should renormalize.
    assert sunday_extension["carbon_g_per_kwh"] == pytest.approx(60.0)


def test_learned_component_weights_are_non_negative_and_normalized_when_history_is_sufficient() -> None:
    historical_rows = []
    timestamps = pd.date_range("2026-01-01 00:00:00", periods=24 * 28, freq="h")
    for ts in timestamps:
        hour_signal = (ts.hour * 3.0) + (ts.dayofweek * 5.0)
        recency_trend = ts.day * 0.4
        historical_rows.append(
            {
                "timestamp": ts,
                "carbon_g_per_kwh": 40.0 + hour_signal + recency_trend,
            }
        )

    historical_df = pd.DataFrame(historical_rows)
    learning_summary = _learn_component_weights(
        historical_df,
        value_column="carbon_g_per_kwh",
    )

    assert learning_summary["method"] == "learned_nnls"
    assert learning_summary["training_rows"] >= 12
    assert learning_summary["validation_rows"] >= 4
    assert pytest.approx(sum(learning_summary["weights"].values())) == 1.0
    assert all(weight >= 0 for weight in learning_summary["weights"].values())
    assert learning_summary["validation_mae"] is not None


def test_learned_component_weights_fall_back_when_history_is_too_sparse() -> None:
    historical_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-03-01 00:00:00", periods=5, freq="h"),
            "carbon_g_per_kwh": [10, 20, 30, 40, 50],
        }
    )

    learning_summary = _learn_component_weights(
        historical_df,
        value_column="carbon_g_per_kwh",
    )

    assert learning_summary["method"] == "fixed_fallback_insufficient_history"
    assert learning_summary["weights"] == {
        "recent_history": 0.45,
        "same_weekday": 0.35,
        "long_run_baseline": 0.20,
    }


def test_live_price_forecast_table_extends_with_historical_pattern(monkeypatch) -> None:
    target_timestamps = pd.Series(pd.date_range("2026-03-21 00:00:00", periods=6, freq="h"))

    live_price_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-03-21 00:00:00",
                    "2026-03-21 01:00:00",
                    "2026-03-21 02:00:00",
                ]
            ),
            "price_per_kwh": [0.20, 0.18, 0.16],
            "source": ["CAISO"] * 3,
            "region_code": ["CAISO"] * 3,
            "price_node": ["NODE"] * 3,
        }
    )

    live_aligned_df = pd.DataFrame(
        {
            "timestamp": target_timestamps,
            "local_time": target_timestamps.dt.strftime("%Y-%m-%d %H:%M:%S"),
            "price_per_kwh": [0.20, 0.18, 0.16, None, None, None],
            "source": ["CAISO"] * 6,
            "region_code": ["CAISO"] * 6,
            "price_node": ["NODE"] * 6,
        }
    )

    historical_price_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-03-19 03:00:00",
                    "2026-03-20 03:00:00",
                    "2026-03-19 04:00:00",
                    "2026-03-20 04:00:00",
                    "2026-03-19 05:00:00",
                    "2026-03-20 05:00:00",
                ]
            ),
            "price_per_kwh": [0.11, 0.11, 0.09, 0.09, 0.07, 0.07],
            "source": ["CAISO"] * 6,
            "region_code": ["CAISO"] * 6,
            "price_node": ["NODE"] * 6,
        }
    )

    call_counter = {"count": 0}

    def fake_get_price_series(*, region_code: str, start_time, end_time):
        call_counter["count"] += 1
        if call_counter["count"] == 1:
            assert start_time == pd.Timestamp("2026-03-21 00:00:00")
            assert end_time == pd.Timestamp("2026-03-21 02:00:00")
        return live_price_df if call_counter["count"] == 1 else historical_price_df

    def fake_align_price_series(*, price_df, target_timestamps, carry_forward_beyond_last_known=True):
        return live_aligned_df.copy()

    monkeypatch.setattr("src.data_fetcher.get_price_series", fake_get_price_series)
    monkeypatch.setattr("src.data_fetcher.align_price_series", fake_align_price_series)

    result = build_live_price_forecast_table(
        region="CAISO_NORTH",
        target_timestamps=target_timestamps,
        live_target_timestamps=target_timestamps.iloc[:3],
        historical_days=2,
        deadline="2026-03-21 05:00:00",
        allow_historical_extension=True,
    )

    extended_rows = result[result["price_signal_source"] == "historical_pattern_estimate"]
    assert extended_rows["timestamp"].tolist() == [
        pd.Timestamp("2026-03-21 03:00:00"),
        pd.Timestamp("2026-03-21 04:00:00"),
        pd.Timestamp("2026-03-21 05:00:00"),
    ]
    assert extended_rows["price_per_kwh"].tolist() == [0.11, 0.09, 0.07]


def test_live_price_forecast_table_preserves_live_rows_when_extension_fails(monkeypatch) -> None:
    target_timestamps = pd.Series(pd.date_range("2026-03-21 00:00:00", periods=5, freq="h"))

    live_price_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-03-21 00:00:00",
                    "2026-03-21 01:00:00",
                    "2026-03-21 02:00:00",
                ]
            ),
            "price_per_kwh": [0.20, 0.18, 0.16],
            "source": ["CAISO"] * 3,
            "region_code": ["CAISO"] * 3,
            "price_node": ["NODE"] * 3,
        }
    )

    live_aligned_df = pd.DataFrame(
        {
            "timestamp": target_timestamps,
            "local_time": target_timestamps.dt.strftime("%Y-%m-%d %H:%M:%S"),
            "price_per_kwh": [0.20, 0.18, 0.16, None, None],
            "source": ["CAISO"] * 5,
            "region_code": ["CAISO"] * 5,
            "price_node": ["NODE"] * 5,
        }
    )

    def fake_get_price_series(*, region_code: str, start_time, end_time):
        return live_price_df

    def fake_align_price_series(*, price_df, target_timestamps, carry_forward_beyond_last_known=True):
        return live_aligned_df.copy()

    def fail_history_template(*, historical_price_df, interval_minutes: float):
        raise ValueError("history template failed")

    monkeypatch.setattr("src.data_fetcher.get_price_series", fake_get_price_series)
    monkeypatch.setattr("src.data_fetcher.align_price_series", fake_align_price_series)
    monkeypatch.setattr("src.data_fetcher._build_historical_price_template", fail_history_template)

    result = build_live_price_forecast_table(
        region="CAISO_NORTH",
        target_timestamps=target_timestamps,
        live_target_timestamps=target_timestamps.iloc[:3],
        historical_days=2,
        deadline="2026-03-21 04:00:00",
        allow_historical_extension=True,
    )

    assert result["price_per_kwh"].tolist()[:3] == [0.20, 0.18, 0.16]
    assert result["price_extension_status"].iloc[0] == "live_only_extension_failed"


def test_live_carbon_forecast_table_preserves_price_output_in_historical_mode(monkeypatch) -> None:
    live_carbon_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-03-21 00:00:00", periods=3, freq="h"),
            "carbon_g_per_kwh": [100, 110, 120],
        }
    )
    extended_carbon_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-03-21 00:00:00", periods=5, freq="h"),
            "carbon_g_per_kwh": [100, 110, 120, 90, 80],
            "carbon_source": [
                "live_forecast",
                "live_forecast",
                "live_forecast",
                "historical_pattern_estimate",
                "historical_pattern_estimate",
            ],
            "historical_avg_carbon_g_per_kwh": [None, None, None, 90, 80],
        }
    )
    price_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-03-21 00:00:00", periods=3, freq="h"),
            "price_per_kwh": [0.20, 0.18, 0.16],
            "price_signal_source": ["live_forecast", "live_forecast", "live_forecast"],
            "price_extension_status": ["live_only_extension_failed"] * 3,
            "price_extension_message": ["extension failed"] * 3,
            "source": ["CAISO"] * 3,
            "region_code": ["CAISO"] * 3,
            "price_node": ["NODE"] * 3,
        }
    )

    monkeypatch.setattr(
        "src.data_fetcher._fetch_live_forecast_with_fallback",
        lambda region: (live_carbon_df.copy(), "CAISO_NORTH", "direct_region"),
    )
    monkeypatch.setattr(
        "src.data_fetcher._fetch_live_historical_with_fallback",
        lambda region, days: (live_carbon_df.copy(), "CAISO_NORTH"),
    )
    monkeypatch.setattr(
        "src.data_fetcher.extend_forecast_with_history",
        lambda live_forecast_df, historical_df, deadline: extended_carbon_df.copy(),
    )
    monkeypatch.setattr(
        "src.data_fetcher.build_live_price_forecast_table",
        lambda **kwargs: price_df.copy(),
    )

    result = build_live_carbon_forecast_table(
        region="CAISO_NORTH",
        carbon_estimation_mode="forecast_plus_historical_expectation",
        historical_days=2,
        deadline="2026-03-21 04:00:00",
        placeholder_price_per_kwh=0.15,
    )

    assert len(result) == 5
    assert result["price_per_kwh"].notna().all()
    assert result["price_per_kwh"].tolist() == [0.20, 0.18, 0.16, 0.15, 0.15]
    assert result["price_signal_source"].tolist()[-2:] == ["placeholder", "placeholder"]
