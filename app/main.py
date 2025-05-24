# time_series_analysis_service/app/main.py
from fastapi import FastAPI, HTTPException, Depends, status # Added Depends, status
from loguru import logger
import sys
import asyncio # Added
from contextlib import asynccontextmanager # Added

from app.models import AnalysisRequest, AnalysisResponse, TimeSeriesData, TimePoint, AnalysisType # Added AnalysisType
from app.config import settings # Use Pydantic settings
from app.services import basic_analyzer, advanced_analyzer
# Import real DB connector and new store function
from app.db_connector.ts_db_connector import (
    connect_db as connect_ts_db, 
    close_db as close_ts_db,
    fetch_time_series_data, # This will now be the real one
    store_analysis_result   # New function to store results
)

logger.remove()
logger.add(sys.stderr, level=settings.LOG_LEVEL.upper()) # Use settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"{settings.SERVICE_NAME} starting up...")
    try:
        await connect_ts_db() # Connect to TimescaleDB
    except Exception as e:
        logger.critical(f"TimescaleDB connection failed during startup: {e}", exc_info=True)
        # Attempt cleanup before raising
        try:
            await close_ts_db()
        except Exception as close_e:
            logger.error(f"Error during TimescaleDB cleanup after connection failure: {close_e}")
        raise RuntimeError(f"TimescaleDB connection failed: {e}") from e
    
    # Add scheduler startup here if you implement a scheduled analysis job later
    # try:
    #     await start_ts_analysis_scheduler() 
    # except Exception as e:
    #     logger.error(f"APScheduler for Time Series Analysis failed to start: {e}", exc_info=True)
    #     # Decide if this is fatal

    logger.info(f"{settings.SERVICE_NAME} startup complete.")
    yield
    logger.info(f"{settings.SERVICE_NAME} shutting down...")
    # await stop_ts_analysis_scheduler() # If scheduler added
    await close_ts_db()
    logger.info("TimescaleDB connection shut down.")
    logger.info(f"{settings.SERVICE_NAME} shutdown complete.")

app = FastAPI(
    title=settings.SERVICE_NAME, # Use settings
    description="Applies statistical models and analyses to time-series data.",
    version="1.0.0",
    lifespan=lifespan
)

# No longer using mock_fetch directly in the endpoint
# async def get_input_timeseries(request: AnalysisRequest) -> TimeSeriesData: ... (see below)

@app.post("/analyze", response_model=AnalysisResponse, summary="Perform Time Series Analysis")
async def analyze_time_series_endpoint(request: AnalysisRequest):
    logger.info(f"Received request for {request.analysis_type.value} on signal '{request.signal_name or 'provided_data'}'")

    input_ts_data: Optional[TimeSeriesData] = None
    effective_signal_name: str

    try:
        if request.time_series_data:
            logger.info("Using time_series_data provided in the request body.")
            input_ts_data = request.time_series_data
            effective_signal_name = input_ts_data.signal_name
        elif request.signal_name and request.start_time and request.end_time:
            logger.info(f"Fetching time series data for signal: {request.signal_name} from DB.")
            # Determine metric column and topic_id if signal_name is structured e.g., "topic_123_document_count"
            # This is a simplification; you might need more robust parsing or explicit params
            parts = request.signal_name.split('_')
            topic_id_filter = None
            metric_to_fetch = "value" # Default if not parsed, or you pass it in request.parameters

            if len(parts) >= 3 and parts[0] == "topic": # e.g. topic_someid_metric
                topic_id_filter = parts[1]
                metric_to_fetch = "_".join(parts[2:])
                # Check if metric_to_fetch is a valid column name in the source signal table
                # For now, we assume it is, or fetch_time_series_data handles this.
            elif len(parts) == 2 and parts[0] == "topic": # e.g. topic_someid -> implies a default metric like document_count
                 topic_id_filter = parts[1]
                 metric_to_fetch = "document_count" # Default metric for a topic
            else: # Treat signal_name as a direct identifier for a simple series or a pre-defined composite
                logger.debug(f"Signal name '{request.signal_name}' not in 'topic_id_metric' format. Assuming it's a direct signal name or fetching default metric.")
                # fetch_time_series_data needs to know how to interpret this generic signal_name

            input_ts_data = await fetch_time_series_data(
                signal_name=request.signal_name, # Pass the original requested name
                topic_id=topic_id_filter,          # Pass parsed topic_id if available
                metric_column=request.parameters.get("metric_column_to_analyze", metric_to_fetch), # Allow override
                start_time=request.start_time,
                end_time=request.end_time
            )
            if not input_ts_data or not input_ts_data.points:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Time series data not found for signal: {request.signal_name} in the given range.")
            effective_signal_name = input_ts_data.signal_name # Use the name from the fetched data
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insufficient parameters to fetch or use time series data.")

        
        result_data: Any = None 
        result_metadata = input_ts_data.metadata.copy() if input_ts_data.metadata else {}
        result_metadata["original_request_signal_name"] = request.signal_name or "provided_data"


        # --- Analysis Logic (calls to basic_analyzer, advanced_analyzer) ---
        if request.analysis_type == AnalysisType.BASIC_STATS:
            result_data = basic_analyzer.calculate_basic_stats(input_ts_data)
            result_metadata["description"] = "Basic descriptive statistics"
        elif request.analysis_type == AnalysisType.MOVING_AVERAGE:
            window = request.parameters.get("window", settings.DEFAULT_MOVING_AVERAGE_WINDOW)
            ma_type = request.parameters.get("type", "simple")
            result_data = advanced_analyzer.calculate_moving_average(input_ts_data, window=window, ma_type=ma_type)
            if result_data: result_metadata["description"] = f"{ma_type.capitalize()} Moving Average with window {window}"
        elif request.analysis_type == AnalysisType.RATE_OF_CHANGE:
            period = request.parameters.get("period", settings.DEFAULT_ROC_PERIOD)
            result_data = basic_analyzer.calculate_rate_of_change(input_ts_data, period=period)
            if result_data: result_metadata["description"] = f"Rate of Change over {period} period(s)"
        elif request.analysis_type == AnalysisType.PERCENT_CHANGE:
            period = request.parameters.get("period", settings.DEFAULT_ROC_PERIOD)
            result_data = basic_analyzer.calculate_percent_change(input_ts_data, period=period)
            if result_data: result_metadata["description"] = f"Percent Change over {period} period(s)"
        elif request.analysis_type == AnalysisType.Z_SCORE:
            window = request.parameters.get("window", settings.DEFAULT_ZSCORE_ROLLING_WINDOW)
            result_data = advanced_analyzer.calculate_z_scores(input_ts_data, window=window)
            if result_data: result_metadata["description"] = f"Z-Scores (rolling window: {window if window else 'None'})"
        elif request.analysis_type == AnalysisType.STL_DECOMPOSITION:
            period = request.parameters.get("period", settings.DEFAULT_STL_PERIOD)
            robust = request.parameters.get("robust", True)
            result_data = advanced_analyzer.perform_stl_decomposition(input_ts_data, period=period, robust=robust)
            if result_data: result_metadata["description"] = f"STL Decomposition (period used: {result_data.period_used if result_data else 'N/A'})" # Guard against None
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported analysis_type: {request.analysis_type.value}")

        if result_data is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Could not compute {request.analysis_type.value}, possibly due to insufficient or unsuitable data for signal '{effective_signal_name}'.")

        # --- Store Analysis Result ---
        try:
            await store_analysis_result(
                original_signal_name=effective_signal_name, # Name of the signal that was analyzed
                analysis_type=request.analysis_type,
                parameters=request.parameters,
                result_data=result_data, # The actual Pydantic model of the result
                result_metadata=result_metadata
            )
            logger.info(f"Analysis result for '{effective_signal_name}' (type: {request.analysis_type.value}) stored.")
        except Exception as db_store_err:
            logger.error(f"Failed to store analysis result for '{effective_signal_name}': {db_store_err}", exc_info=True)
            # Decide if API call should fail if storage fails. For now, return result.

        return AnalysisResponse(
            requested_signal_name=effective_signal_name,
            analysis_type=request.analysis_type,
            input_parameters=request.parameters,
            result=result_data,
            result_metadata=result_metadata
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Error processing analysis request for signal '{request.signal_name or 'provided_data'}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal server error during time series analysis.")


if __name__ == "__main__":
    import uvicorn
    # The mock_fetch override is removed as we intend to use the real DB connector.
    # For isolated local testing of just the API/logic without a live DB, you could re-add it.
    
    logger.info(f"Starting {settings.SERVICE_NAME} for local development on port 8003...")
    service_port = 8003 # Matching Dockerfile
    # try: service_port = settings.SERVICE_PORT # If you add SERVICE_PORT to settings
    # except AttributeError: pass
    uvicorn.run(
        "app.main:app", 
        host="0.0.0.0", 
        port=service_port, 
        log_level=settings.LOG_LEVEL.lower(), 
        reload=True
    )