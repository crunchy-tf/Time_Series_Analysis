# time_series_analysis_service/app/main_processor.py
import asyncio
from loguru import logger
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple # Added Tuple

from app.config import settings
from app.db_connector.ts_db_connector import (
    fetch_time_series_data,
    store_analysis_result,
    get_distinct_topic_signals_to_analyze,
    get_latest_analysis_timestamp # To potentially avoid re-analysis
)
from app.models import TimeSeriesData, AnalysisRequest, AnalysisType # AnalysisRequest not used directly by scheduler job but by helper
from app.services import basic_analyzer, advanced_analyzer

async def run_and_store_scheduled_analysis(
    topic_id_str: str,           # e.g., "123"
    metric_to_analyze: str,    # e.g., "document_count"
    analysis_type: AnalysisType,
    analysis_params: dict,
    start_dt: datetime,
    end_dt: datetime
):
    """
    Helper to fetch data for a specific topic_id & metric, run a specific analysis, and store it.
    """
    # Construct the signal_name used by fetch_time_series_data and for storing results
    # This should match how the API endpoint might construct it or how you want to identify it
    # For scheduled tasks, we are more direct: "topic_{id}_{metric}"
    signal_name_for_fetching = f"topic_{topic_id_str}" # Base identifier for fetch_time_series_data if it parses metric
    original_signal_name_for_results = f"topic_{topic_id_str}_{metric_to_analyze}" # Full name for storing
    
    logger.info(f"Scheduler: Processing {analysis_type.value} for topic '{topic_id_str}', metric '{metric_to_analyze}' from {start_dt} to {end_dt}.")

    input_ts_data = await fetch_time_series_data(
        signal_name=signal_name_for_fetching, # Pass base identifier "topic_123"
        topic_id=topic_id_str,                # Explicitly pass topic_id
        metric_column=metric_to_analyze,
        start_time=start_dt,
        end_time=end_dt
    )

    if not input_ts_data or not input_ts_data.points:
        logger.debug(f"Scheduler: No data for topic '{topic_id_str}', metric '{metric_to_analyze}' in range for {analysis_type.value}.")
        return

    result_data = None
    result_metadata = input_ts_data.metadata.copy() if input_ts_data.metadata else {}
    result_metadata["analysis_source"] = "scheduled_job"
    result_metadata["scheduler_run_timestamp"] = datetime.now(timezone.utc).isoformat()


    if analysis_type == AnalysisType.MOVING_AVERAGE:
        window = analysis_params.get("window", settings.DEFAULT_MOVING_AVERAGE_WINDOW)
        ma_type = analysis_params.get("type", "simple")
        result_data = advanced_analyzer.calculate_moving_average(input_ts_data, window=window, ma_type=ma_type)
        if result_data: result_metadata["description"] = f"Scheduled {ma_type.capitalize()} MA (w={window})"
    
    elif analysis_type == AnalysisType.Z_SCORE:
        window = analysis_params.get("window", settings.DEFAULT_ZSCORE_ROLLING_WINDOW)
        result_data = advanced_analyzer.calculate_z_scores(input_ts_data, window=window)
        if result_data: result_metadata["description"] = f"Scheduled Z-Scores (w={window if window else 'None'})"
    
    elif analysis_type == AnalysisType.STL_DECOMPOSITION:
        period = analysis_params.get("period", settings.DEFAULT_STL_PERIOD)
        robust = analysis_params.get("robust", True) # Default robust to True for scheduler
        result_data = advanced_analyzer.perform_stl_decomposition(input_ts_data, period=period, robust=robust)
        if result_data and result_data.period_used is not None: 
            result_metadata["description"] = f"Scheduled STL (p={result_data.period_used})"
        elif result_data:
             result_metadata["description"] = f"Scheduled STL (period not definitively set)"
    
    # Add more analysis types from TSA_SCHEDULED_ANALYSES if needed
    # Example: Basic Stats
    elif analysis_type == AnalysisType.BASIC_STATS:
        result_data = basic_analyzer.calculate_basic_stats(input_ts_data)
        if result_data: result_metadata["description"] = "Scheduled Basic descriptive statistics"
    
    else:
        logger.warning(f"Scheduler: Analysis type '{analysis_type.value}' not configured for scheduled execution in run_and_store_scheduled_analysis.")
        return

    if result_data:
        await store_analysis_result(
            original_signal_name=original_signal_name_for_results, # Store with full identifying name
            analysis_type=analysis_type,
            parameters=analysis_params,
            result_data=result_data,
            result_metadata=result_metadata
        )
        logger.info(f"Scheduler: Stored {analysis_type.value} for topic '{topic_id_str}', metric '{metric_to_analyze}'.")
    else:
        logger.warning(f"Scheduler: Could not compute {analysis_type.value} for topic '{topic_id_str}', metric '{metric_to_analyze}'.")


async def scheduled_ts_analysis_job():
    """
    Periodically fetches distinct signals (topic_id, metric_name) and runs predefined analyses.
    """
    logger.info("--- Starting scheduled Time Series Analysis job ---")
    job_start_time = datetime.now(timezone.utc)

    # distinct_signals is List[Tuple[topic_id_str, metric_name_str]]
    distinct_signals_to_check: List[Tuple[str, str]] = await get_distinct_topic_signals_to_analyze(
        limit=settings.TSA_SCHEDULER_BATCH_SIZE
    )

    if not distinct_signals_to_check:
        logger.info("Scheduler: No distinct signals found to analyze in this cycle.")
        logger.info(f"--- Scheduled Time Series Analysis job finished. Duration: {(datetime.now(timezone.utc) - job_start_time).total_seconds():.2f}s ---")
        return

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=settings.TSA_SCHEDULER_LOOKBACK_DAYS)

    analysis_tasks = []

    for topic_id, metric in distinct_signals_to_check:
        for analysis_name_str in settings.TSA_SCHEDULED_ANALYSES:
            try:
                analysis_type_enum = AnalysisType(analysis_name_str)
            except ValueError:
                logger.warning(f"Scheduler: Invalid analysis type '{analysis_name_str}' in TSA_SCHEDULED_ANALYSES settings. Skipping.")
                continue
            
            # Construct full signal name for checking last analysis time
            # This name should match what's stored as 'original_signal_name' in analysis_results table
            full_signal_name_id = f"topic_{topic_id}_{metric}"

            # Optional: Check last analysis time to decide if re-analysis is needed.
            # This simple version always re-analyzes the lookback window.
            # For a more optimized version, you'd fetch data since `last_analysis_ts`.
            # last_analysis_ts = await get_latest_analysis_timestamp(
            #     original_signal_name=full_signal_name_id,
            #     analysis_type_str=analysis_type_enum.value
            # )
            # if last_analysis_ts and (end_dt - last_analysis_ts) < timedelta(hours=1): # Example: don't re-run if done in last hour
            #     logger.debug(f"Scheduler: Skipping {analysis_type_enum.value} for {full_signal_name_id}, recently analyzed at {last_analysis_ts}.")
            #     continue

            params_for_analysis = {} # Specific params per analysis type for scheduler
            if analysis_type_enum == AnalysisType.MOVING_AVERAGE:
                params_for_analysis = {"window": settings.DEFAULT_MOVING_AVERAGE_WINDOW, "type": "simple"}
            elif analysis_type_enum == AnalysisType.Z_SCORE:
                params_for_analysis = {"window": settings.DEFAULT_ZSCORE_ROLLING_WINDOW}
            elif analysis_type_enum == AnalysisType.STL_DECOMPOSITION:
                 params_for_analysis = {"period": settings.DEFAULT_STL_PERIOD, "robust": True}
            # Add other scheduled analyses here if their default params differ or are needed

            analysis_tasks.append(
                run_and_store_scheduled_analysis(
                    topic_id_str=topic_id,
                    metric_to_analyze=metric,
                    analysis_type=analysis_type_enum,
                    analysis_params=params_for_analysis,
                    start_dt=start_dt,
                    end_dt=end_dt
                )
            )

    if analysis_tasks:
        logger.info(f"Scheduler: Created {len(analysis_tasks)} analysis tasks to run.")
        results = await asyncio.gather(*analysis_tasks, return_exceptions=True)
        for i, res_or_exc in enumerate(results):
            if isinstance(res_or_exc, Exception):
                # Log which task failed. Need to associate res_or_exc back to the task's details.
                # This is tricky without more context on what `analysis_tasks[i]` was.
                # For now, a generic error.
                logger.error(f"Scheduler: An error occurred in one of the analysis tasks: {res_or_exc}", exc_info=res_or_exc)
    else:
        logger.info("Scheduler: No analysis tasks were scheduled to run in this cycle (after filtering).")

    job_duration = (datetime.now(timezone.utc) - job_start_time).total_seconds()
    logger.info(f"--- Scheduled Time Series Analysis job finished. Duration: {job_duration:.2f}s ---")