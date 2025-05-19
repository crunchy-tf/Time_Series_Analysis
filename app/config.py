# time_series_analysis_service/app/config.py
from typing import List, Optional

LOG_LEVEL: str = "INFO"

# Default parameters for analysis, can be overridden in requests
DEFAULT_MOVING_AVERAGE_WINDOW: int = 7
DEFAULT_ROC_PERIOD: int = 1
DEFAULT_ZSCORE_ROLLING_WINDOW: Optional[int] = None # None means use whole series
DEFAULT_STL_PERIOD: Optional[int] = None # e.g., 7 for daily data with weekly seasonality; None lets statsmodels try to infer or use defaults

# Placeholder for actual database connection details if not using a mock
# TIMESCALEDB_HOST: str = "localhost"
# TIMESCALEDB_PORT: int = 5432
# TIMESCALEDB_USER: str = "user"
# TIMESCALEDB_PASSWORD: str = "password"
# TIMESCALEDB_DBNAME: str = "minbar_ts_data"