# time_series_analysis_service/app/main.py
from fastapi import FastAPI, HTTPException, Depends, status 
from loguru import logger
import sys
import asyncio 
from contextlib import asynccontextmanager 

from app.models import AnalysisRequest, AnalysisResponse, TimeSeriesData, TimePoint, AnalysisType 
from app.config import settings 
from app.services import basic_analyzer, advanced_analyzer
from app.db_connector.ts_db_connector import (
    connect_db as connect_ts_db, 
    close_db as close_ts_db,
    fetch_time_series_data, 
    store_analysis_result   
)
# --- IMPORT CORRECT SCHEDULER FUNCTIONS ---
from app.services.scheduler_service import start_ts_analysis_scheduler, stop_ts_analysis_scheduler

logger.remove()
logger.add(sys.stderr, level=settings.LOG_LEVEL.upper()) 

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"{settings.SERVICE_NAME} starting up...")
    try:
        await connect_ts_db() 
    except Exception as e:
        logger.critical(f"TimescaleDB connection failed during startup: {e}", exc_info=True)
        try: await close_ts_db()
        except Exception as close_e: logger.error(f"Error during DB cleanup: {close_e}")
        raise RuntimeError(f"TimescaleDB connection failed: {e}") from e
    
    try:
        await start_ts_analysis_scheduler() # Correct function name
    except Exception as e:
        logger.error(f"APScheduler for Time Series Analysis failed to start: {e}", exc_info=True)
        # Optionally, make this fatal by re-raising or exiting

    logger.info(f"{settings.SERVICE_NAME} startup complete.")
    yield
    logger.info(f"{settings.SERVICE_NAME} shutting down...")
    await stop_ts_analysis_scheduler() # Correct function name
    await close_ts_db()
    logger.info("Scheduler and TimescaleDB connection shut down.") 
    logger.info(f"{settings.SERVICE_NAME} shutdown complete.")

app = FastAPI(
    title=settings.SERVICE_NAME, 
    description="Applies statistical models and analyses to time-series data.",
    version="1.0.0",
    lifespan=lifespan
)

@app.post("/analyze", response_model=AnalysisResponse, summary="Perform Time Series Analysis")
async def analyze_time_series_endpoint(request: AnalysisRequest):
    logger.info(f"Received API request for {request.analysis_type.value} on signal '{request.signal_name or 'provided_data'}'")

    input_ts_data: Optional[TimeSeriesData] = None
    effective_signal_name: str

    try:
        if request.time_series_data:
            logger.info("Using time_series_data provided in the request body.")
            input_ts_data = request.time_series_data
            effective_signal_name = input_ts_data.signal_name
        elif request.signal_name and request.start_time and request.end_time:
            logger.info(f"Fetching time series data for signal: {request.signal_name} from DB.")
            
            parts = request.signal_name.split('_')
            topic_id_filter = None
            # Default metric_to_fetch; the API parameter `metric_column_to_analyze` takes precedence if provided
            metric_to_fetch_default = "document_count" 
            
            if len(parts) >= 2 and parts[0] == "topic": # e.g. topic_123 OR topic_123_some_metric
                topic_id_filter = parts[1]
                if len(parts) >= 3: # topic_123_some_metric
                    metric_to_fetch_default = "_".join(parts[2:])
            
            metric_column_final = request.parameters.get("metric_column_to_analyze", metric_to_fetch_default)

            input_ts_data = await fetch_time_series_data(
                signal_name=request.signal_name, 
                topic_id=topic_id_filter,          
                metric_column=metric_column_final, 
                start_time=request.start_time,
                end_time=request.end_time
            )
            if not input_ts_data or not input_ts_data.points:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Time series data not found for signal: {request.signal_name} (metric: {metric_column_final}, topic: {topic_id_filter}) in the given range.")
            effective_signal_name = input_ts_data.signal_name 
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Insufficient parameters: provide time_series_data or (signal_name, start_time, end_time).")

        result_data: Any = None 
        result_metadata = input_ts_data.metadata.copy() if input_ts_data.metadata else {}
        result_metadata["original_request_signal_name"] = request.signal_name or "provided_data"
        result_metadata["analysis_source"] = "api_call"


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
            if result_data and result_data.period_used is not None : result_metadata["description"] = f"STL Decomposition (period used: {result_data.period_used})"
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported analysis_type: {request.analysis_type.value}")

        if result_data is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Could not compute {request.analysis_type.value} for signal '{effective_signal_name}'.")

        try:
            await store_analysis_result(
                original_signal_name=effective_signal_name, 
                analysis_type=request.analysis_type,
                parameters=request.parameters,
                result_data=result_data, 
                result_metadata=result_metadata
            )
            logger.info(f"API Analysis result for '{effective_signal_name}' (type: {request.analysis_type.value}) stored.")
        except Exception as db_store_err:
            logger.error(f"Failed to store API analysis result for '{effective_signal_name}': {db_store_err}", exc_info=True)

        return AnalysisResponse(
            requested_signal_name=effective_signal_name,
            analysis_type=request.analysis_type,
            input_parameters=request.parameters,
            result=result_data,
            result_metadata=result_metadata
        )
    except HTTPException as http_exc: raise http_exc
    except Exception as e:
        logger.error(f"Error processing API analysis request for signal '{request.signal_name or 'provided_data'}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal server error during time series analysis.")

if __name__ == "__main__":
    import uvicorn
    service_port = 8003 
    uvicorn.run( "app.main:app", host="0.0.0.0", port=service_port, log_level=settings.LOG_LEVEL.lower(), reload=True)