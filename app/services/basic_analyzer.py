# time_series_analysis_service/app/services/basic_analyzer.py
from typing import List, Optional
import numpy as np
from loguru import logger

from app.models import TimeSeriesData, TimePoint, BasicStatsResult

def calculate_basic_stats(ts_data: TimeSeriesData) -> Optional[BasicStatsResult]:
    if not ts_data.points:
        logger.warning(f"Cannot calculate basic stats for signal '{ts_data.signal_name}': No data points.")
        return None
    
    values = [p.value for p in ts_data.points]
    if not values: # Should be caught by the above, but as a safeguard
        return None

    np_values = np.array(values)
    
    return BasicStatsResult(
        count=len(np_values),
        sum_val=float(np.sum(np_values)),
        mean=float(np.mean(np_values)),
        median=float(np.median(np_values)),
        min_val=float(np.min(np_values)),
        max_val=float(np.max(np_values)),
        std_dev=float(np.std(np_values)),
        variance=float(np.var(np_values))
    )

def calculate_rate_of_change(ts_data: TimeSeriesData, period: int = 1) -> Optional[TimeSeriesData]:
    if not ts_data.points or len(ts_data.points) <= period:
        logger.warning(f"Not enough data points in signal '{ts_data.signal_name}' for ROC with period {period}.")
        return None

    roc_points: List[TimePoint] = []
    for i in range(period, len(ts_data.points)):
        roc_value = ts_data.points[i].value - ts_data.points[i-period].value
        roc_points.append(TimePoint(timestamp=ts_data.points[i].timestamp, value=roc_value))
    
    return TimeSeriesData(
        signal_name=f"{ts_data.signal_name}_roc_{period}",
        points=roc_points,
        metadata={"original_signal": ts_data.signal_name, "roc_period": period}
    )

def calculate_percent_change(ts_data: TimeSeriesData, period: int = 1) -> Optional[TimeSeriesData]:
    if not ts_data.points or len(ts_data.points) <= period:
        logger.warning(f"Not enough data points in signal '{ts_data.signal_name}' for Percent Change with period {period}.")
        return None

    pc_points: List[TimePoint] = []
    for i in range(period, len(ts_data.points)):
        prev_value = ts_data.points[i-period].value
        if prev_value == 0: # Avoid division by zero
            # Decide how to handle: skip, or assign a large number, or specific flag
            logger.warning(f"Previous value is zero at index {i-period} for signal '{ts_data.signal_name}', cannot calculate percent change.")
            pc_value = np.nan # Or some other indicator
        else:
            pc_value = ((ts_data.points[i].value - prev_value) / prev_value) * 100.0
        
        if not np.isnan(pc_value): # Only append if calculation was valid
            pc_points.append(TimePoint(timestamp=ts_data.points[i].timestamp, value=pc_value))
            
    return TimeSeriesData(
        signal_name=f"{ts_data.signal_name}_pct_change_{period}",
        points=pc_points,
        metadata={"original_signal": ts_data.signal_name, "change_period": period}
    )