"""
Global constants used across the Util project.
"""

# -------------------------------------------------------------------
# DEVELOPMENT STATUS
# -------------------------------------------------------------------

# True while using simulated data for development
USING_PLACEHOLDER_DATA = True


# -------------------------------------------------------------------
# Supported optimization objectives
# -------------------------------------------------------------------

SUPPORTED_OBJECTIVES = ["cost", "carbon"]


# -------------------------------------------------------------------
# Machine assumptions
# -------------------------------------------------------------------

DEFAULT_MACHINE_WATTS = 300


# -------------------------------------------------------------------
# Forecast assumptions
# -------------------------------------------------------------------

HOURS_PER_DAY = 24
FORECAST_HORIZON_HOURS = 48