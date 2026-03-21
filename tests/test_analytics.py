from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.analytics import (
    ANALYTICS_COLUMNS,
    append_run,
    clear_analytics_data,
    filter_analytics_data,
    load_analytics_data,
    summarize_analytics,
)


def test_load_analytics_data_handles_missing_file(tmp_path: Path) -> None:
    analytics_path = tmp_path / "analytics.csv"

    df = load_analytics_data(analytics_path)

    assert list(df.columns) == ANALYTICS_COLUMNS
    assert df.empty


def test_append_run_persists_expected_fields(tmp_path: Path) -> None:
    analytics_path = tmp_path / "analytics.csv"

    append_run(
        analytics_path,
        {
            "timestamp": "2026-03-20T10:00:00",
            "run_type": "Real",
            "compute_hours": 4,
            "region": "CAISO_NORTH",
            "carbon_saved": 1.25,
            "cost_saved": 0.42,
            "schedule_mode": "Flexible",
        },
    )

    df = load_analytics_data(analytics_path)

    assert len(df) == 1
    assert df.iloc[0]["run_type"] == "Real"
    assert df.iloc[0]["region"] == "CAISO_NORTH"
    assert df.iloc[0]["carbon_saved"] == 1.25
    assert "notes" in df.columns


def test_filter_analytics_data_excludes_test_runs_by_default(tmp_path: Path) -> None:
    analytics_path = tmp_path / "analytics.csv"
    append_run(analytics_path, {"timestamp": "2026-03-20T10:00:00", "run_type": "Real", "region": "A"})
    append_run(analytics_path, {"timestamp": "2026-03-21T10:00:00", "run_type": "Test", "region": "B"})

    df = load_analytics_data(analytics_path)
    filtered = filter_analytics_data(df, include_test_runs=False)

    assert len(filtered) == 1
    assert filtered.iloc[0]["run_type"] == "Real"


def test_summarize_analytics_handles_real_and_test_runs(tmp_path: Path) -> None:
    analytics_path = tmp_path / "analytics.csv"
    append_run(
        analytics_path,
        {
            "timestamp": "2026-03-20T10:00:00",
            "run_type": "Real",
            "compute_hours": 2,
            "carbon_saved": 4.0,
            "cost_saved": 1.5,
            "carbon_reduction_pct": 10,
            "selected_interval_count": 6,
        },
    )
    append_run(
        analytics_path,
        {
            "timestamp": "2026-03-20T11:00:00",
            "run_type": "Test",
            "compute_hours": 1,
            "carbon_saved": 2.0,
            "cost_saved": 0.5,
            "carbon_reduction_pct": 20,
            "selected_interval_count": 4,
        },
    )

    summary = summarize_analytics(load_analytics_data(analytics_path))

    assert summary["total_logged_runs"] == 2
    assert summary["total_real_runs"] == 1
    assert summary["total_test_runs"] == 1
    assert summary["total_compute_hours"] == 3.0
    assert summary["total_carbon_saved"] == 6.0
    assert summary["total_cost_saved"] == 2.0
    assert summary["avg_carbon_saved_per_real_run"] == 4.0


def test_clear_analytics_data_resets_file(tmp_path: Path) -> None:
    analytics_path = tmp_path / "analytics.csv"
    append_run(analytics_path, {"timestamp": "2026-03-20T10:00:00", "run_type": "Real"})

    clear_analytics_data(analytics_path)
    cleared = load_analytics_data(analytics_path)

    assert cleared.empty
    assert analytics_path.exists()
