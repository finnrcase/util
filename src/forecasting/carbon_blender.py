from __future__ import annotations

import pandas as pd

from src.forecasting.pattern_extension import (
    DEFAULT_TOTAL_HORIZON_DAYS,
    extend_series_with_history,
)


def extend_forecast_with_history(
    live_forecast_df: pd.DataFrame,
    historical_df: pd.DataFrame,
    deadline: str | pd.Timestamp,
    total_horizon_days: int | None = DEFAULT_TOTAL_HORIZON_DAYS,
) -> pd.DataFrame:
    """
    Extend the live carbon forecast using a historical time-of-day expectation.
    """
    return extend_series_with_history(
        live_forecast_df=live_forecast_df,
        historical_df=historical_df,
        deadline=deadline,
        value_column="carbon_g_per_kwh",
        source_column="carbon_source",
        live_source_value="live_forecast",
        historical_source_value="historical_pattern_estimate",
        profile_value_column="historical_avg_carbon_g_per_kwh",
        total_horizon_days=total_horizon_days,
    )
