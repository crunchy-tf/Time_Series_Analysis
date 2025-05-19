# time_series_analysis_service/app/models.py
from pydantic import BaseModel, Field, validator
from typing import List, Dict, Optional, Any, Union
from datetime import datetime
from enum import Enum

class TimePoint(BaseModel):
    timestamp: datetime
    value: float

class TimeSeriesData(BaseModel):
    signal_name: str
    points: List[TimePoint]
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

class AnalysisType(str, Enum):
    BASIC_STATS = "basic_stats"
    MOVING_AVERAGE = "moving_average"
    RATE_OF_CHANGE = "rate_of_change"
    PERCENT_CHANGE = "percent_change"
    Z_SCORE = "z_score"
    STL_DECOMPOSITION = "stl_decomposition"

class AnalysisRequest(BaseModel):
    # Option 1: Provide data directly (for stateless calls or testing)
    time_series_data: Optional[TimeSeriesData] = None
    
    # Option 2: Parameters to fetch data (more common for a service)
    signal_name: Optional[str] = None 
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    analysis_type: AnalysisType
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @validator('time_series_data', always=True)
    def check_data_or_fetch_params(cls, v, values):
        if v is None and not (values.get('signal_name') and values.get('start_time') and values.get('end_time')):
            raise ValueError('Either time_series_data or (signal_name, start_time, end_time) must be provided')
        if v is not None and (values.get('signal_name') or values.get('start_time') or values.get('end_time')):
            raise ValueError('Provide either time_series_data directly or fetch parameters (signal_name, start_time, end_time), not both.')
        return v

# --- Result Models for Specific Analyses ---

class BasicStatsResult(BaseModel):
    count: int
    sum_val: float
    mean: float
    median: float
    min_val: float
    max_val: float
    std_dev: float
    variance: float

class MovingAverageResult(BaseModel):
    # original_signal_name: str # Not needed if passed within result_metadata
    moving_average_signal: TimeSeriesData
    window: int
    type: str # e.g., "simple", "exponential"

class ZScorePoint(BaseModel):
    timestamp: datetime
    original_value: float
    z_score: Optional[float] # Optional if std_dev is zero for the window

class ZScoreResult(BaseModel):
    # original_signal_name: str
    points: List[ZScorePoint]
    mean_used: Optional[float] = None # Overall mean if not rolling
    std_dev_used: Optional[float] = None # Overall std_dev if not rolling
    window: Optional[int] = None

class STLDecompositionResult(BaseModel):
    # original_signal_name: str
    original_timestamps: List[datetime]
    trend: List[Optional[float]]
    seasonal: List[Optional[float]]
    residual: List[Optional[float]]
    period_used: Optional[int] = None

# Union type for the 'result' field in the main response
AnalysisResultData = Union[
    BasicStatsResult,
    MovingAverageResult, 
    ZScoreResult, 
    STLDecompositionResult,
    TimeSeriesData # For analyses that return a new time series (e.g., ROC, PercentChange)
]

class AnalysisResponse(BaseModel):
    requested_signal_name: str
    analysis_type: AnalysisType
    input_parameters: Dict[str, Any]
    result: AnalysisResultData
    status: str = "success"
    message: Optional[str] = None
    result_metadata: Optional[Dict[str, Any]] = Field(default_factory=dict) # e.g., units, description of result