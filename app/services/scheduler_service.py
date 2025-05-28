# time_series_analysis_service/app/services/scheduler_service.py
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from app.config import settings
from app.main_processor import scheduled_ts_analysis_job # Import the job from main_processor

scheduler = AsyncIOScheduler(timezone="UTC")
_scheduler_started = False

async def start_ts_analysis_scheduler():
    global _scheduler_started
    if scheduler.running or _scheduler_started:
        logger.info("Time Series Analysis APScheduler already running or start initiated.")
        return
    
    # Use SCHEDULER_INTERVAL_MINUTES from settings (which might be aliased)
    job_interval = settings.SCHEDULER_INTERVAL_MINUTES 
    logger.info(f"Configuring TSA scheduler with interval: {job_interval} minutes.")

    try:
        scheduler.add_job(
            scheduled_ts_analysis_job, 
            trigger=IntervalTrigger(minutes=job_interval), # Use the fetched interval
            id="ts_analysis_job",
            name="Perform Scheduled Time Series Analyses",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=int(job_interval * 60 * 0.5) # e.g., 50% of interval
        )
        scheduler.start()
        _scheduler_started = True
        logger.success(f"Time Series Analysis APScheduler started. Job scheduled every {job_interval} minutes.")
    except Exception as e:
        logger.error(f"Failed to start Time Series Analysis APScheduler: {e}", exc_info=True)
        _scheduler_started = False

async def stop_ts_analysis_scheduler():
    global _scheduler_started
    if scheduler.running:
        logger.info("Stopping Time Series Analysis APScheduler...")
        scheduler.shutdown(wait=False)
        _scheduler_started = False
        logger.success("Time Series Analysis APScheduler stopped.")
    else:
        logger.info("Time Series Analysis APScheduler was not running.")