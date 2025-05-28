# time_series_analysis_service/app/main_processor.py
import asyncio
from loguru import logger
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

from app.config import settings
from app.db_connector.ts_db_connector import (
    fetch_time_series_data,
    store_analysis_result,
    get_distinct_topic_signals_to_analyze,
    get_latest_analysis_timestamp 
)
from app.models import TimeSeriesData, AnalysisType # Removed AnalysisRequest, not directly used by scheduler job
from app.services import basic_analyzer, advanced_analyzer

async def run_and_store_scheduled_analysis(
    topic_id_str: str,
    metric_to_analyze: str,
    analysis_type: AnalysisType,
    analysis_params: dict,
    start_dt: datetime,
    end_dt: datetime
):
    """
    Helper to fetch data for a specific topic_id & metric, run a specific analysis, and store it.
    """
    original_signal_name_for_results = f"topic_{topic_id_str}_{metric_to_analyze}"
    
    logger.info(f"Scheduler: Processing {analysis_type.value} for topic '{topic_id_str}', metric '{metric_to_analyze}' from {start_dt} to {end_dt}.")

    input_ts_data = await fetch_time_series_data(
        signal_name=f"topic_{topic_id_str}", 
        topic_id=topic_id_str,
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

    if analysis_type == AnalysisType.BASIC_STATS:
        result_data = basic_analyzer.calculate_basic_stats(input_ts_data)
        if result_data: result_metadata["description"] = "Scheduled Basic descriptive statistics"
    
    elif analysis_type == AnalysisType.MOVING_AVERAGE:
        window = analysis_params.get("window", settings.DEFAULT_MOVING_AVERAGE_WINDOW)
        ma_type = analysis_params.get("type", "simple")
        result_data = advanced_analyzer.calculate_moving_average(input_ts_data, window=window, ma_type=ma_type)
        if result_data: result_metadata["description"] = f"Scheduled {ma_type.capitalize()} MA (w={window})"
    
    elif analysis_type == AnalysisType.RATE_OF_CHANGE:
        period = analysis_params.get("period", settings.DEFAULT_ROC_PERIOD)
        result_data = basic_analyzer.calculate_rate_of_change(input_ts_data, period=period)
        if result_data: result_metadata["description"] = f"Scheduled Rate of Change (p={period})"

    elif analysis_type == AnalysisType.PERCENT_CHANGE:
        period = analysis_params.get("period", settings.DEFAULT_ROC_PERIOD)
        result_data = basic_analyzer.calculate_percent_change(input_ts_data, period=period)
        if result_data: result_metadata["description"] = f"Scheduled Percent Change (p={period})"

    elif analysis_type == AnalysisType.Z_SCORE:
        window = analysis_params.get("window", settings.DEFAULT_ZSCORE_ROLLING_WINDOW)
        result_data = advanced_analyzer.calculate_z_scores(input_ts_data, window=window)
        if result_data: result_metadata["description"] = f"Scheduled Z-Scores (w={window if window else 'None'})"
    
    elif analysis_type == AnalysisType.STL_DECOMPOSITION:
        period = analysis_params.get("period", settings.DEFAULT_STL_PERIOD)
        robust = analysis_params.get("robust", True)
        result_data = advanced_analyzer.perform_stl_decomposition(input_ts_data, period=period, robust=robust)
        if result_data and result_data.period_used is not None: 
            result_metadata["description"] = f"Scheduled STL (p={result_data.period_used})"
        elif result_data:
             result_metadata["description"] = f"Scheduled STL (period not definitively set)"
    
    else: # Should not be reached if TSA_SCHEDULED_ANALYSES only contains valid enum members
        logger.error(f"Scheduler: Unhandled AnalysisType '{analysis_type.value}' in run_and_store_scheduled_analysis.")
        return

    if result_data:
        await store_analysis_result(
            original_signal_name=original_signal_name_for_results,
            analysis_type=analysis_type,
            parameters=analysis_params,
            result_data=result_data,
            result_metadata=result_metadata
        )
        logger.info(f"Scheduler: Stored {analysis_type.value} for topic '{topic_id_str}', metric '{metric_to_analyze}'.")
    else:
        logger.warning(f"Scheduler: Could not compute {analysis_type.value} for topic '{topic_id_str}', metric '{metric_to_analyze}'.")


async def scheduled_ts_analysis_job():
    logger.info("--- Starting scheduled Time Series Analysis job ---")
    job_start_time = datetime.now(timezone.utc)

    distinct_signals_to_check: List[Tuple[str, str]] = await get_distinct_topic_signals_to_analyze(
        limit=settings.TSA_SCHEDULER_BATCH_SIZE
    )

    if not distinct_signals_to_check:
        logger.info("Scheduler: No distinct signals found to analyze in this cycle.")
        job_duration_no_data = (datetime.now(timezone.utc) - job_start_time).total_seconds()
        logger.info(f"--- Scheduled Time Series Analysis job finished (no signals to process). Duration: {job_duration_no_data:.2f}s ---")
        return

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=settings.TSA_SCHEDULER_LOOKBACK_DAYS)
    analysis_tasks = []

    for topic_id, metric in distinct_signals_to_check:
        # settings.TSA_SCHEDULED_ANALYSES now defaults to all AnalysisType members
        for analysis_name_str in settings.TSA_SCHEDULED_ANALYSES:
            try:
                analysis_type_enum = AnalysisType(analysis_name_str)
            except ValueError:
                # This should be rare now due to validator in config.py if .env is used,
                # but good to keep for direct modification of settings object.
                logger.warning(f"Scheduler: Invalid analysis type string '{analysis_name_str}' encountered. Skipping.")
                continue
            
            params_for_this_run = {} # Default empty dict
            # Set specific default parameters for scheduled runs if needed
            if analysis_type_enum == AnalysisType.MOVING_AVERAGE:
                params_for_this_run = {"window": settings.DEFAULT_MOVING_AVERAGE_WINDOW, "type": "simple"}
            elif analysis_type_enum == AnalysisType.RATE_OF_CHANGE or analysis_type_enum == AnalysisType.PERCENT_CHANGE:
                params_for_this_run = {"period": settings.DEFAULT_ROC_PERIOD}
            elif analysis_type_enum == AnalysisType.Z_SCORE:
                params_for_this_run = {"window": settings.DEFAULT_ZSCORE_ROLLING_WINDOW}
            elif analysis_type_enum == AnalysisType.STL_DECOMPOSITION:
                 params_for_this_run = {"period": settings.DEFAULT_STL_PERIOD, "robust": True}
            # BASIC_STATS takes no parameters in its current implementation for scheduled runs

            analysis_tasks.append(
                run_and_store_scheduled_analysis(
                    topic_id_str=topic_id,
                    metric_to_analyze=metric,
                    analysis_type=analysis_type_enum,
                    analysis_params=params_for_this_run,
                    start_dt=start_dt,
                    end_dt=end_dt
                )
            )

    if analysis_tasks:
        logger.info(f"Scheduler: Created {len(analysis_tasks)} analysis tasks to run.")
        results = await asyncio.gather(*analysis_tasks, return_exceptions=True)
        for i, res_or_exc in enumerate(results):
            if isinstance(res_or_exc, Exception):
                logger.error(f"Scheduler: An error occurred in an analysis task: {res_or_exc}", exc_info=res_or_exc)
    else:
        logger.info("Scheduler: No analysis tasks were scheduled to run (e.g., no distinct signals or no configured analyses).")

    job_duration = (datetime.now(timezone.utc) - job_start_time).total_seconds()
    logger.info(f"--- Scheduled Time Series Analysis job finished. Duration: {job_duration:.2f}s ---")