# time_series_analysis_service/app/config.py
from typing import List, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict # For Pydantic V2
from pydantic import Field # For Pydantic V2

class Settings(BaseSettings):
    SERVICE_NAME: str = "Minbar Time Series Analysis Service"
    LOG_LEVEL: str = "INFO"

    # --- TimescaleDB Connection ---
    TIMESCALEDB_USER: str
    TIMESCALEDB_PASSWORD: str
    TIMESCALEDB_HOST: str
    TIMESCALEDB_PORT: int = Field(default=5432)
    TIMESCALEDB_DB: str

    # --- Table Naming Conventions ---
    SOURCE_SIGNALS_TABLE_PREFIX: str = Field(default="agg_signals")
    ANALYSIS_RESULTS_TABLE_PREFIX: str = Field(default="analysis_results")

    # --- Default parameters for analysis ---
    DEFAULT_MOVING_AVERAGE_WINDOW: int = 7
    DEFAULT_ROC_PERIOD: int = 1
    DEFAULT_ZSCORE_ROLLING_WINDOW: Optional[int] = None
    DEFAULT_STL_PERIOD: Optional[int] = None

    # Optional: Scheduler settings if added later
    # SCHEDULER_INTERVAL_MINUTES: int = Field(default=120, gt=0)

    @property
    def timescaledb_dsn_asyncpg(self) -> str:
        return f"postgresql://{self.TIMESCALEDB_USER}:{self.TIMESCALEDB_PASSWORD}@{self.TIMESCALEDB_HOST}:{self.TIMESCALEDB_PORT}/{self.TIMESCALEDB_DB}"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()

# Update global vars if your other modules still use them (prefer settings object)
LOG_LEVEL = settings.LOG_LEVEL
DEFAULT_MOVING_AVERAGE_WINDOW = settings.DEFAULT_MOVING_AVERAGE_WINDOW
DEFAULT_ROC_PERIOD = settings.DEFAULT_ROC_PERIOD
DEFAULT_ZSCORE_ROLLING_WINDOW = settings.DEFAULT_ZSCORE_ROLLING_WINDOW
DEFAULT_STL_PERIOD = settings.DEFAULT_STL_PERIOD