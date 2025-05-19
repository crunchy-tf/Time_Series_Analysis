# time_series_analysis_service/app/main.py
from fastapi import FastAPI, HTTPException
from typing import Dict, Any
from loguru import logger
import sys

from app.models import AnalysisRequest, AnalysisResponse, TimeSeriesData, TimePoint # Ensure all used models are imported
from app.config import LOG_LEVEL
from app.services import basic_analyzer, advanced_analyzer # Import your service modules
from app.db_connector.ts_db_connector import fetch_time_series_data # Import your (mocked) DB connector

# --- Logger Configuration ---
logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL)

app = FastAPI(
    title="Minbar Time Series Analysis Service",
    description="Applies statistical models and analyses to time-series data.",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    logger.info("Time Series Analysis Service starting up...")
    logger.info("Time Series Analysis Service startup complete.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Time Series Analysis Service shutting down...")
    logger.info("Time Series Analysis Service shutdown complete.")

# Helper to get data, using mock for now
def get_input_timeseries(request: AnalysisRequest) -> TimeSeriesData:
    if request.time_series_data:
        logger.info("Using time_series_data provided in the request body.")
        return request.time_series_data
    elif request.signal_name and request.start_time and request.end_time:
        logger.info(f"Fetching time series data for signal: {request.signal_name} from DB.")
        ts_data = fetch_time_series_data(
            signal_name=request.signal_name,
            start_time=request.start_time,
            end_time=request.end_time
        )
        if not ts_data or not ts_data.points:
            raise HTTPException(status_code=404, detail=f"Time series data not found for signal: {request.signal_name} in the given range.")
        return ts_data
    else:
        # This case should be caught by Pydantic validator in AnalysisRequest
        raise HTTPException(status_code=400, detail="Insufficient parameters to fetch or use time series data.")


@app.post("/analyze", response_model=AnalysisResponse, summary="Perform Time Series Analysis")
async def analyze_time_series_endpoint(request: AnalysisRequest):
    logger.info(f"Received request for {request.analysis_type} on signal '{request.signal_name or 'provided_data'}'")

    try:
        input_ts_data = get_input_timeseries(request)
        
        result_data: Any = None # To hold the specific result model
        result_metadata = input_ts_data.metadata.copy() if input_ts_data.metadata else {} # Start with input metadata

        if request.analysis_type == AnalysisType.BASIC_STATS:
            result_data = basic_analyzer.calculate_basic_stats(input_ts_data)
            result_metadata["description"] = "Basic descriptive statistics"
        
        elif request.analysis_type == AnalysisType.MOVING_AVERAGE:
            window = request.parameters.get("window", 7)
            ma_type = request.parameters.get("type", "simple")
            if not isinstance(window, int) or window <= 0:
                raise HTTPException(status_code=400, detail="Invalid 'window' parameter for moving_average.")
            
            # calculate_moving_average returns a MovingAverageResult model directly
            result_data = advanced_analyzer.calculate_moving_average(input_ts_data, window=window, ma_type=ma_type)
            if result_data: # It returns MovingAverageResult which contains the TimeSeriesData
                 result_metadata["description"] = f"{ma_type.capitalize()} Moving Average with window {window}"
                 # The result_data is already the correct Pydantic model for the union type

        elif request.analysis_type == AnalysisType.RATE_OF_CHANGE:
            period = request.parameters.get("period", 1)
            if not isinstance(period, int) or period <= 0:
                raise HTTPException(status_code=400, detail="Invalid 'period' parameter for rate_of_change.")
            # calculate_roc returns TimeSeriesData
            result_data = basic_analyzer.calculate_rate_of_change(input_ts_data, period=period)
            if result_data: result_metadata["description"] = f"Rate of Change over {period} period(s)"

        elif request.analysis_type == AnalysisType.PERCENT_CHANGE:
            period = request.parameters.get("period", 1)
            if not isinstance(period, int) or period <= 0:
                raise HTTPException(status_code=400, detail="Invalid 'period' parameter for percent_change.")
            # calculate_percent_change returns TimeSeriesData
            result_data = basic_analyzer.calculate_percent_change(input_ts_data, period=period)
            if result_data: result_metadata["description"] = f"Percent Change over {period} period(s)"

        elif request.analysis_type == AnalysisType.Z_SCORE:
            window = request.parameters.get("window") # Optional
            if window and (not isinstance(window, int) or window <= 0):
                 raise HTTPException(status_code=400, detail="Invalid 'window' parameter for z_score.")
            # calculate_z_scores returns ZScoreResult
            result_data = advanced_analyzer.calculate_z_scores(input_ts_data, window=window)
            if result_data: result_metadata["description"] = f"Z-Scores (rolling window: {window if window else 'None'})"
        
        elif request.analysis_type == AnalysisType.STL_DECOMPOSITION:
            period = request.parameters.get("period") # Optional
            robust = request.parameters.get("robust", True)
            if period and (not isinstance(period, int) or period <= 1):
                 raise HTTPException(status_code=400, detail="Invalid 'period' parameter for stl_decomposition. Must be > 1.")
            # perform_stl_decomposition returns STLDecompositionResult
            result_data = advanced_analyzer.perform_stl_decomposition(input_ts_data, period=period, robust=robust)
            if result_data: result_metadata["description"] = f"STL Decomposition (period used: {result_data.period_used})"

        else:
            raise HTTPException(status_code=400, detail=f"Unsupported analysis_type: {request.analysis_type.value}")

        if result_data is None: # If a specific analysis function returned None due to data issues
            raise HTTPException(status_code=422, detail=f"Could not compute {request.analysis_type.value}, possibly due to insufficient or unsuitable data.")

        return AnalysisResponse(
            requested_signal_name=input_ts_data.signal_name, # Use signal name from fetched/provided data
            analysis_type=request.analysis_type,
            input_parameters=request.parameters,
            result=result_data,
            result_metadata=result_metadata
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Error processing analysis request for signal '{request.signal_name or 'provided_data'}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during time series analysis: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    # Ensure the mock connector is used if running directly
    from app.db_connector.ts_db_connector import fetch_time_series_data as mock_fetch
    app.dependency_overrides[fetch_time_series_data] = lambda: mock_fetch # Example of override for local test

    logger.info(f"Starting Time Series Analysis Service for local development on port 8003...")
    uvicorn.run(app, host="0.0.0.0", port=8003, log_level=LOG_LEVEL.lower())