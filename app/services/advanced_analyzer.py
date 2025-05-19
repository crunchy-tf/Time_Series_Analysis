# time_series_analysis_service/app/services/advanced_analyzer.py
from typing import List, Optional, Dict, Any
import numpy as np
import pandas as pd # For moving averages and easier time series handling
from statsmodels.tsa.seasonal import STL # For STL decomposition
from loguru import logger

from app.models import TimeSeriesData, TimePoint, MovingAverageResult, ZScorePoint, ZScoreResult, STLDecompositionResult
from app.config import DEFAULT_MOVING_AVERAGE_WINDOW, DEFAULT_ZSCORE_ROLLING_WINDOW, DEFAULT_STL_PERIOD


def calculate_moving_average(ts_data: TimeSeriesData, window: int, ma_type: str = "simple") -> Optional[MovingAverageResult]:
    if not ts_data.points or len(ts_data.points) < window:
        logger.warning(f"Not enough data points in signal '{ts_data.signal_name}' for moving average with window {window}.")
        return None

    df = pd.DataFrame([p.model_dump() for p in ts_data.points])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp')

    ma_series: Optional[pd.Series] = None
    if ma_type.lower() == "simple":
        ma_series = df['value'].rolling(window=window).mean()
    elif ma_type.lower() == "exponential":
        ma_series = df['value'].ewm(span=window, adjust=False).mean()
    else:
        logger.error(f"Unsupported moving average type: {ma_type} for signal '{ts_data.signal_name}'.")
        return None
    
    ma_points: List[TimePoint] = []
    if ma_series is not None:
        for ts, val in ma_series.items():
            if not pd.isna(val): # Skip NaN values (usually at the beginning of MA)
                ma_points.append(TimePoint(timestamp=ts, value=float(val)))
                
    return MovingAverageResult(
        moving_average_signal=TimeSeriesData(
            signal_name=f"{ts_data.signal_name}_{ma_type.lower()}_ma_{window}",
            points=ma_points,
            metadata={"original_signal": ts_data.signal_name, "window": window, "type": ma_type}
        ),
        window=window,
        type=ma_type
    )


def calculate_z_scores(ts_data: TimeSeriesData, window: Optional[int] = DEFAULT_ZSCORE_ROLLING_WINDOW) -> Optional[ZScoreResult]:
    if not ts_data.points:
        logger.warning(f"No data points in signal '{ts_data.signal_name}' to calculate Z-scores.")
        return None

    values = np.array([p.value for p in ts_data.points])
    timestamps = [p.timestamp for p in ts_data.points]
    z_score_points: List[ZScorePoint] = []
    
    overall_mean: Optional[float] = None
    overall_std: Optional[float] = None

    if window is None or window <= 0 or window > len(values): # Calculate Z-score based on whole series
        mean = np.mean(values)
        std = np.std(values)
        overall_mean = float(mean)
        overall_std = float(std)
        if std == 0:
            logger.warning(f"Standard deviation is zero for entire signal '{ts_data.signal_name}'. Z-scores will be NaN or 0.")
            z_scores = np.zeros_like(values) if mean == 0 else np.full_like(values, np.nan)
        else:
            z_scores = (values - mean) / std
        
        for i, z in enumerate(z_scores):
            z_score_points.append(ZScorePoint(timestamp=timestamps[i], original_value=values[i], z_score=None if np.isnan(z) else float(z)))

    else: # Rolling Z-score
        logger.info(f"Calculating rolling Z-score with window {window} for signal '{ts_data.signal_name}'.")
        series = pd.Series(values)
        rolling_mean = series.rolling(window=window, min_periods=1).mean() # Use min_periods=1 to get values for initial points
        rolling_std = series.rolling(window=window, min_periods=1).std()
        
        for i in range(len(values)):
            mean_val = rolling_mean.iloc[i]
            std_val = rolling_std.iloc[i]
            
            if pd.isna(std_val) or std_val == 0:
                z = np.nan # Cannot calculate Z-score if std is 0 or NaN
            else:
                z = (values[i] - mean_val) / std_val
            z_score_points.append(ZScorePoint(timestamp=timestamps[i], original_value=values[i], z_score=None if np.isnan(z) else float(z)))
            
    return ZScoreResult(
        points=z_score_points,
        mean_used=overall_mean,
        std_dev_used=overall_std,
        window=window
    )


def perform_stl_decomposition(ts_data: TimeSeriesData, period: Optional[int] = DEFAULT_STL_PERIOD, robust: bool = True) -> Optional[STLDecompositionResult]:
    if not ts_data.points or len(ts_data.points) < (2 * (period or 7)): # Need at least 2 full periods for STL
        logger.warning(f"Not enough data points in signal '{ts_data.signal_name}' for STL decomposition. Need at least 2 periods ({2*(period or 7)} points).")
        return None

    df = pd.DataFrame([p.model_dump() for p in ts_data.points])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp')['value'] # Ensure it's a Pandas Series with DatetimeIndex

    # If period is not provided, try to infer or use a common default (e.g., 7 for daily data -> weekly)
    # Statsmodels STL requires a period if the index doesn't have a frequency.
    # If your data is regularly spaced (e.g., daily), Pandas might infer frequency.
    
    actual_period = period
    if actual_period is None:
        # Attempt to infer frequency, or default
        if df.index.freq:
            # Basic inference, may need adjustment based on actual data freq (e.g., 'D' -> 7, 'H' -> 24)
            # This part can be complex; for now, if no period, we might just use a common default or require it
            logger.warning(f"STL period not specified for signal '{ts_data.signal_name}'. Defaulting based on inferred frequency or common value might be inaccurate.")
            # A simple default if no frequency is inferred:
            actual_period = 7 # Assuming daily data, weekly seasonality as a common case
        else: # If no frequency can be inferred from DatetimeIndex
            if len(df) >= 14 : # A heuristic
                 actual_period = 7
                 logger.warning(f"No frequency in DatetimeIndex and no period specified for STL. Defaulting to period={actual_period} for '{ts_data.signal_name}'.")
            else:
                 logger.error(f"Cannot perform STL on '{ts_data.signal_name}': period not specified and cannot be robustly inferred for short series.")
                 return None


    try:
        logger.info(f"Performing STL decomposition for signal '{ts_data.signal_name}' with period {actual_period}.")
        stl_result = STL(df, period=actual_period, robust=robust).fit()
        
        # Ensure all components have the same length as original timestamps
        # STL components might be shorter if NaNs were at the ends
        trend = [float(x) if not pd.isna(x) else None for x in stl_result.trend.reindex(df.index).tolist()]
        seasonal = [float(x) if not pd.isna(x) else None for x in stl_result.seasonal.reindex(df.index).tolist()]
        resid = [float(x) if not pd.isna(x) else None for x in stl_result.resid.reindex(df.index).tolist()]

        return STLDecompositionResult(
            original_timestamps=[p.timestamp for p in ts_data.points],
            trend=trend,
            seasonal=seasonal,
            residual=resid,
            period_used=actual_period
        )
    except Exception as e:
        logger.error(f"Error during STL decomposition for signal '{ts_data.signal_name}': {e}", exc_info=True)
        return None