# time_series_analysis_service/app/config.py
from typing import List, Optional, Any # Added Any
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator # Added field_validator
from loguru import logger # For logging within the validator if needed

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

    @field_validator('DEFAULT_ZSCORE_ROLLING_WINDOW', 'DEFAULT_STL_PERIOD', mode='before')
    @classmethod
    def parse_optional_int_from_env(cls, v: Any) -> Optional[int]:
        """
        Handles empty strings from .env for Optional[int] fields, converting them to None.
        Allows actual integer strings to be parsed by Pydantic's default mechanism.
        """
        if isinstance(v, str):
            v_stripped = v.strip()
            if v_stripped == "":
                logger.trace(f"Validator: Received empty string for Optional[int], converting to None.")
                return None
            # If it's a non-empty string, let Pydantic try to parse it as an int.
            # Pydantic will raise a ValidationError if it's not a valid integer string.
            # No explicit int(v_stripped) here to avoid double parsing or masking Pydantic's error.
            return v_stripped # Return the stripped string for Pydantic to handle
        
        if v is None: # If not set in .env, it will be None already (due to default)
            logger.trace(f"Validator: Received None for Optional[int], returning None.")
            return None
            
        if isinstance(v, int): # If somehow it's already an int (e.g. direct instantiation)
            logger.trace(f"Validator: Received int value {v} for Optional[int], returning as is.")
            return v
            
        # For any other type, let Pydantic's default validation handle it (which will likely raise an error)
        logger.trace(f"Validator: Received unexpected type {type(v)} value '{v}' for Optional[int], passing to Pydantic.")
        return v

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()

# These global re-assignments are generally not the preferred pattern with Pydantic settings.
# It's better for other modules to import `settings` and use `settings.LOG_LEVEL`, etc.
# However, keeping them if your other modules rely on these globals for now.
LOG_LEVEL = settings.LOG_LEVEL
DEFAULT_MOVING_AVERAGE_WINDOW = settings.DEFAULT_MOVING_AVERAGE_WINDOW
DEFAULT_ROC_PERIOD = settings.DEFAULT_ROC_PERIOD
DEFAULT_ZSCORE_ROLLING_WINDOW = settings.DEFAULT_ZSCORE_ROLLING_WINDOW
DEFAULT_STL_PERIOD = settings.DEFAULT_STL_PERIOD

# Example of how other modules should use it:
# from app.config import settings
# logger.info(f"Using Z-score window: {settings.DEFAULT_ZSCORE_ROLLING_WINDOW}")