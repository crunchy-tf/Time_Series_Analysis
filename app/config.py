# time_series_analysis_service/app/config.py
from typing import List, Optional, Any 
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator
from loguru import logger
import json 

# Import your AnalysisType enum to easily define the default
from app.models import AnalysisType # Make sure this path is correct for your models.py

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
    TSA_SCHEDULER_BATCH_SIZE: int = Field(default=10, gt=0, description="Number of distinct signals (topic_id/metric) to process per scheduler run")
    TSA_SCHEDULER_LOOKBACK_DAYS: int = Field(default=30, gt=0, description="How many days back to fetch data for scheduled analysis")
    
    # --- MODIFIED DEFAULT FOR TSA_SCHEDULED_ANALYSES ---
    TSA_SCHEDULED_ANALYSES: List[str] = Field(
        default_factory=lambda: [analysis.value for analysis in AnalysisType] # Use all enum members
    )
    # --- END MODIFICATION ---


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
        # Default if parsing from .env fails or if .env value is invalid/empty
        default_analyses_all = [analysis.value for analysis in AnalysisType]
        
        if isinstance(v, list):
            if all(isinstance(item, str) for item in v):
                valid_types = [at.value for at in AnalysisType]
                validated_list = [item for item in v if item in valid_types]
                if len(validated_list) != len(v):
                    logger.warning(f"TSA_SCHEDULED_ANALYSES list from env contains invalid AnalysisTypes. Using only valid ones or default (all).")
                    return validated_list if validated_list else default_analyses_all
                return validated_list
            else:
                logger.warning(f"TSA_SCHEDULED_ANALYSES list from env non-string. Using default (all): {default_analyses_all}")
                return default_analyses_all
        if isinstance(v, str):
            if not v.strip():
                logger.debug(f"TSA_SCHEDULED_ANALYSES is empty string in env. Using default (all): {default_analyses_all}")
                return default_analyses_all
            try:
                parsed_list = json.loads(v)
                if isinstance(parsed_list, list) and all(isinstance(item, str) for item in parsed_list):
                    valid_types = [at.value for at in AnalysisType]
                    validated_list = [item for item in parsed_list if item in valid_types]
                    if len(validated_list) != len(parsed_list):
                        logger.warning(f"TSA_SCHEDULED_ANALYSES JSON from env invalid AnalysisTypes. Using only valid ones or default (all).")
                        return validated_list if validated_list else default_analyses_all
                    return validated_list
                else:
                    logger.warning(f"TSA_SCHEDULED_ANALYSES from env ('{v}') not valid JSON list of strings. Using default (all): {default_analyses_all}")
                    return default_analyses_all
            except json.JSONDecodeError:
                logger.warning(f"Could not parse TSA_SCHEDULED_ANALYSES JSON from env: '{v}'. Using default (all): {default_analyses_all}")
                return default_analyses_all
        
        logger.debug(f"TSA_SCHEDULED_ANALYSES not explicitly set or invalid type in env. Using default (all): {default_analyses_all}")
        return default_analyses_all

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()

LOG_LEVEL = settings.LOG_LEVEL
DEFAULT_MOVING_AVERAGE_WINDOW = settings.DEFAULT_MOVING_AVERAGE_WINDOW
DEFAULT_ROC_PERIOD = settings.DEFAULT_ROC_PERIOD
DEFAULT_ZSCORE_ROLLING_WINDOW = settings.DEFAULT_ZSCORE_ROLLING_WINDOW
DEFAULT_STL_PERIOD = settings.DEFAULT_STL_PERIOD