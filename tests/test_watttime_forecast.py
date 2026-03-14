import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.watttime_service import get_forecast, forecast_to_dataframe

print("Requesting carbon forecast from WattTime...")

forecast_json = get_forecast(region="CAISO_NORTH", signal_type="co2_moer")

if isinstance(forecast_json, dict) and "data" in forecast_json:
    print("Raw forecast rows returned:", len(forecast_json["data"]))
else:
    print("Raw forecast rows returned:", len(forecast_json))

forecast_df = forecast_to_dataframe(forecast_json)

print("\nFirst rows of normalized dataframe:")
print(forecast_df.head())

print("\nColumns:")
print(forecast_df.columns.tolist())