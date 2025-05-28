# time_series_analysis_service/app/config.py
from typing import List, Optional, Any 
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator
from loguru import logger
import json # For TSA_SCHEDULED_ANALYSES validator

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

    # --- Default parameters for analysis (API calls) ---
    DEFAULT_MOVING_AVERAGE_WINDOW: int = 7
    DEFAULT_ROC_PERIOD: int = 1
    DEFAULT_ZSCORE_ROLLING_WINDOW: Optional[int] = None
    DEFAULT_STL_PERIOD: Optional[int] = None

    # --- Scheduler settings for Time Series Analysis ---
    # Maps to SCHEDULER_INTERVAL_MINUTES_TSA in .env if you use that, or SCHEDULER_INTERVAL_MINUTES
    SCHEDULER_INTERVAL_MINUTES: int = Field(default=60, gt=0, validation_alias='SCHEDULER_INTERVAL_MINUTES_TSA')
    TSA_SCHEDULER_BATCH_SIZE: int = Field(default=10, gt=0)
    TSA_SCHEDULER_LOOKBACK_DAYS: int = Field(default=90, gt=0)
    TSA_SCHEDULED_ANALYSES: List[str] = Field(default_factory=lambda: ["moving_average", "z_score"])


    @property
    def timescaledb_dsn_asyncpg(self) -> str:
        return f"postgresql://{self.TIMESCALEDB_USER}:{self.TIMESCALEDB_PASSWORD}@{self.TIMESCALEDB_HOST}:{self.TIMESCALEDB_PORT}/{self.TIMESCALEDB_DB}"

    @field_validator('DEFAULT_ZSCORE_ROLLING_WINDOW', 'DEFAULT_STL_PERIOD', mode='before')
    @classmethod
    def parse_optional_int_from_env(cls, v: Any) -> Optional[int]:
        if isinstance(v, str):
            v_stripped = v.strip()
            if v_stripped == "":
                logger.trace(f"Validator: Received empty string for Optional[int], converting to None.")
                return None
            return v_stripped 
        if v is None:
            logger.trace(f"Validator: Received None for Optional[int], returning None.")
            return None
        if isinstance(v, int):
            logger.trace(f"Validator: Received int value {v} for Optional[int], returning as is.")
            return v
        logger.trace(f"Validator: Received unexpected type {type(v)} value '{v}' for Optional[int], passing to Pydantic.")
        return v

    @field_validator('TSA_SCHEDULED_ANALYSES', mode='before')
    @classmethod
    def parse_scheduled_analyses_from_env(cls, v: Any) -> List[str]:
        default_analyses = ["moving_average", "z_score"]
        if isinstance(v, list):
            # Ensure all items are strings
            if all(isinstance(item, str) for item in v):
                return v
            else:
                logger.warning(f"TSA_SCHEDULED_ANALYSES list contains non-string items. Using default: {default_analyses}")
                return default_analyses
        if isinstance(v, str):
            if not v.strip(): # Empty string in env
                logger.info(f"TSA_SCHEDULED_ANALYSES is empty string in env. Using default: {default_analyses}")
                return default_analyses
            try:
                parsed_list = json.loads(v)
                if isinstance(parsed_list, list) and all(isinstance(item, str) for item in parsed_list):
                    return parsed_list
                else:
                    logger.warning(f"TSA_SCHEDULED_ANALYSES from env ('{v}') is not a valid JSON list of strings. Using default: {default_analyses}")
                    return default_analyses
            except json.JSONDecodeError:
                logger.warning(f"Could not parse TSA_SCHEDULED_ANALYSES JSON string from env: '{v}'. Using default: {default_analyses}")
                return default_analyses
        
        # Fallback if type is unexpected or if it's None and default_factory didn't catch it (shouldn't happen with default_factory)
        logger.warning(f"TSA_SCHEDULED_ANALYSES has unexpected type or value: {type(v)}. Using default: {default_analyses}")
        return default_analyses

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()

# For modules that might import these directly (though settings.FIELD is preferred)
LOG_LEVEL = settings.LOG_LEVEL
DEFAULT_MOVING_AVERAGE_WINDOW = settings.DEFAULT_MOVING_AVERAGE_WINDOW
DEFAULT_ROC_PERIOD = settings.DEFAULT_ROC_PERIOD
DEFAULT_ZSCORE_ROLLING_WINDOW = settings.DEFAULT_ZSCORE_ROLLING_WINDOW
DEFAULT_STL_PERIOD = settings.DEFAULT_STL_PERIOD