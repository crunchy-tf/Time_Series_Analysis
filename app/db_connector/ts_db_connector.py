# time_series_analysis_service/app/db_connector/ts_db_connector.py
import asyncpg
from datetime import datetime, timezone, timedelta # Added timedelta
from typing import Optional, List, Dict, Any, Tuple # Added Tuple
from loguru import logger
import json 

from app.config import settings 
from app.models import TimeSeriesData, TimePoint, AnalysisResultData, AnalysisType
from app.models import BasicStatsResult, MovingAverageResult, ZScoreResult, STLDecompositionResult

_pool: Optional[asyncpg.Pool] = None
_tables_checked: set = set() 

async def connect_db():
    global _pool
    if _pool and not getattr(_pool, '_closed', True):
        logger.debug("TimescaleDB connection pool already established.")
        return

    logger.info(f"Connecting to TimescaleDB: {settings.timescaledb_dsn_asyncpg}")
    try:
        _pool = await asyncpg.create_pool(
            dsn=settings.timescaledb_dsn_asyncpg,
            min_size=2,
            max_size=10
        )
        logger.success("TimescaleDB connection pool established.")
    except Exception as e:
        logger.critical(f"Failed to connect to TimescaleDB: {e}", exc_info=True)
        _pool = None
        raise ConnectionError("Could not connect to TimescaleDB") from e

async def close_db():
    global _pool, _tables_checked
    if _pool:
        logger.info("Closing TimescaleDB connection pool.")
        await _pool.close()
        _pool = None
        _tables_checked = set()
        logger.success("TimescaleDB connection pool closed.")

async def get_pool() -> asyncpg.Pool:
    if _pool is None or getattr(_pool, '_closed', True):
        await connect_db()
    if _pool is None:
        raise ConnectionError("TimescaleDB pool unavailable.")
    return _pool

async def _ensure_analysis_results_table_exists(table_name: str, time_column_name: str = "analysis_timestamp"):
    global _tables_checked
    if table_name in _tables_checked:
        return

    pool = await get_pool()
    logger.info(f"Checking/Creating TimescaleDB hypertable '{table_name}' for analysis results...")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        analysis_timestamp TIMESTAMPTZ NOT NULL,
        original_signal_name TEXT NOT NULL,
        analysis_type TEXT NOT NULL,
        parameters JSONB,
        result_value_numeric DOUBLE PRECISION,
        result_series_jsonb JSONB,
        result_structured_jsonb JSONB,
        metadata JSONB,
        PRIMARY KEY (original_signal_name, analysis_type, analysis_timestamp)
    );
    """
    create_hypertable_sql = f"SELECT create_hypertable('{table_name}', '{time_column_name}', if_not_exists => TRUE, migrate_data => TRUE);"

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(create_table_sql)
                logger.info(f"Standard table '{table_name}' for analysis results ensured.")
                try:
                    await conn.execute(create_hypertable_sql)
                    logger.success(f"Hypertable '{table_name}' for analysis results ensured/created.")
                except asyncpg.PostgresError as ts_err:
                    if "already a hypertable" in str(ts_err).lower():
                        logger.info(f"Table '{table_name}' is already a hypertable.")
                    elif "extension \"timescaledb\" does not exist" in str(ts_err).lower():
                         logger.error(f"TimescaleDB extension not enabled in database '{settings.TIMESCALEDB_DB}'. Cannot create hypertable '{table_name}'.")
                         raise
                    else:
                        logger.error(f"Error converting '{table_name}' to hypertable: {ts_err}", exc_info=True)
        _tables_checked.add(table_name)
    except Exception as e:
        logger.error(f"Error during setup of analysis results table/hypertable '{table_name}': {e}", exc_info=True)
        raise

async def fetch_time_series_data(
    signal_name: str,
    start_time: datetime, 
    end_time: datetime,
    topic_id: Optional[str] = None, 
    metric_column: str = "document_count"
) -> Optional[TimeSeriesData]:
    pool = await get_pool()
    source_table_name = settings.SOURCE_SIGNALS_TABLE_PREFIX + "_topic_hourly" 
    
    logger.info(f"Fetching data for signal '{signal_name}' (metric: {metric_column} from table {source_table_name}, topic: {topic_id}) from {start_time} to {end_time}.")

    query_conditions = ["signal_timestamp >= $1", "signal_timestamp < $2"] # Use < $2 for exclusive end
    query_params: list = [start_time, end_time]
    
    actual_topic_id = topic_id
    # This logic is for when the API endpoint calls, scheduler logic might be more direct
    if actual_topic_id is None and "topic_" in signal_name: 
        parts = signal_name.split("_")
        if parts[0] == "topic" and len(parts) >= 2:
            actual_topic_id = parts[1]

    if actual_topic_id:
        query_conditions.append(f"topic_id = ${len(query_params) + 1}")
        query_params.append(str(actual_topic_id)) 

    safe_metric_column = "".join(c for c in metric_column if c.isalnum() or c == '_')
    if not safe_metric_column or safe_metric_column != metric_column:
        logger.error(f"Invalid or potentially unsafe metric_column provided: {metric_column}")
        return None

    query = f"""
        SELECT signal_timestamp as timestamp, "{safe_metric_column}" as value 
        FROM "{source_table_name}"
        WHERE {" AND ".join(query_conditions)}
        ORDER BY signal_timestamp ASC;
    """
    
    points = []
    try:
        records = await pool.fetch(query, *query_params)
        for record in records:
            if record['value'] is not None: 
                points.append(TimePoint(timestamp=record['timestamp'], value=float(record['value'])))
        
        if not points:
            logger.debug(f"No data points found for signal '{signal_name}' (metric: {safe_metric_column}, topic: {actual_topic_id}) in range [{start_time} to {end_time}].")
            return None
            
        logger.info(f"Fetched {len(points)} data points for signal '{signal_name}'.")
        
        ts_data_signal_name = signal_name 
        if actual_topic_id: 
             ts_data_signal_name = f"topic_{actual_topic_id}_{safe_metric_column}"

        return TimeSeriesData(
            signal_name=ts_data_signal_name, 
            points=points, 
            metadata={
                "source_table": source_table_name, 
                "metric_column_used": safe_metric_column, 
                "topic_id_filter_used": actual_topic_id,
                "original_requested_signal_name": signal_name,
                "requested_metric_column": metric_column 
            }
        )
    except asyncpg.exceptions.UndefinedTableError:
        logger.error(f"Source signal table '{source_table_name}' does not exist in TimescaleDB.")
        return None
    except asyncpg.exceptions.UndefinedColumnError as e:
        logger.error(f"Column undefined error fetching from '{source_table_name}' (metric: '{safe_metric_column}', topic_id relevant?): {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching time series data for '{signal_name}': {e}", exc_info=True)
        return None

async def store_analysis_result(
    original_signal_name: str, 
    analysis_type: AnalysisType, 
    parameters: Dict[str, Any],
    result_data: AnalysisResultData,
    result_metadata: Optional[Dict[str, Any]] = None
):
    pool = await get_pool()
    table_name = f"{settings.ANALYSIS_RESULTS_TABLE_PREFIX}_{analysis_type.value.lower()}" 
    
    await _ensure_analysis_results_table_exists(table_name)
    
    record_to_insert = {
        "analysis_timestamp": datetime.now(timezone.utc),
        "original_signal_name": original_signal_name,
        "analysis_type": analysis_type.value,
        "parameters": json.dumps(parameters) if parameters else None,
        "result_value_numeric": None,
        "result_series_jsonb": None,
        "result_structured_jsonb": None,
        "metadata": json.dumps(result_metadata) if result_metadata else None
    }

    if isinstance(result_data, BasicStatsResult):
        record_to_insert["result_structured_jsonb"] = json.dumps(result_data.model_dump())
    elif isinstance(result_data, MovingAverageResult):
        record_to_insert["result_series_jsonb"] = json.dumps(result_data.moving_average_signal.model_dump())
    elif isinstance(result_data, ZScoreResult):
        record_to_insert["result_structured_jsonb"] = json.dumps(result_data.model_dump(exclude_none=True))
    elif isinstance(result_data, STLDecompositionResult):
        dumpable_stl = result_data.model_dump(exclude_none=True)
        # Pydantic v2: .model_dump() handles datetimes to isoformat by default if configured in model,
        # otherwise ensure manual conversion if needed for JSON serialization
        if 'original_timestamps' in dumpable_stl and isinstance(dumpable_stl['original_timestamps'], list):
             dumpable_stl["original_timestamps"] = [ts.isoformat() if isinstance(ts, datetime) else ts for ts in dumpable_stl['original_timestamps']]
        record_to_insert["result_structured_jsonb"] = json.dumps(dumpable_stl)
    elif isinstance(result_data, TimeSeriesData):
        record_to_insert["result_series_jsonb"] = json.dumps(result_data.model_dump())
    else:
        logger.warning(f"Unhandled result_data type for storage: {type(result_data)}")
        try:
            record_to_insert["result_structured_jsonb"] = json.dumps(result_data) 
        except TypeError:
            logger.error(f"Could not serialize result_data of type {type(result_data)} to JSON.")
            return

    cols = list(record_to_insert.keys())
    vals_placeholders = ", ".join([f"${i+1}" for i in range(len(cols))])
    conflict_target = "(original_signal_name, analysis_type, analysis_timestamp)" 
    update_setters = ", ".join([f"\"{col}\" = EXCLUDED.\"{col}\"" for col in cols if col not in ["original_signal_name", "analysis_type", "analysis_timestamp"]])

    insert_query = f"""
        INSERT INTO "{table_name}" ({", ".join(f'"{c}"' for c in cols)}) 
        VALUES ({vals_placeholders})
        ON CONFLICT {conflict_target} DO UPDATE SET {update_setters}; 
    """
    
    try:
        async with pool.acquire() as conn:
            await conn.execute(insert_query, *record_to_insert.values())
        logger.info(f"Stored/Updated analysis result for '{original_signal_name}', type '{analysis_type.value}' in '{table_name}'.")
    except Exception as e:
        logger.error(f"Error storing analysis result in '{table_name}': {e}", exc_info=True)

# --- NEW FUNCTIONS FOR SCHEDULER ---
async def get_distinct_topic_signals_to_analyze(limit: int) -> List[Tuple[str, str]]:
    """
    Fetches distinct topic_ids from the source signals table.
    The associated metric to analyze for these topics is assumed to be 'document_count'
    by the calling function (main_processor.py) for this iteration.
    Returns a list of tuples: (topic_id_as_string, metric_name_to_analyze)
    """
    pool = await get_pool()
    source_table = f"{settings.SOURCE_SIGNALS_TABLE_PREFIX}_topic_hourly" # Source table
    
    # We only want topic_ids that have actual data.
    # We also want to avoid analyzing the outlier topic (-1) too proactively unless specified.
    query = f"""
        SELECT DISTINCT topic_id
        FROM "{source_table}"
        WHERE topic_id IS NOT NULL 
          AND topic_id != '-1' -- Optionally exclude outlier, or make this configurable
          -- AND document_count IS NOT NULL AND document_count > 0 -- Ensure there's data to analyze for this metric
        ORDER BY topic_id -- Or by MAX(signal_timestamp) DESC if you want freshest topics
        LIMIT $1;
    """
    try:
        records = await pool.fetch(query, limit)
        # Each record will have a 'topic_id' field.
        # The "signal_name_identifier" will be "topic_{topic_id}"
        # The metric to analyze will be "document_count" by default for scheduled runs.
        distinct_signals = [(str(record['topic_id']), "document_count") for record in records]
        logger.info(f"Found {len(distinct_signals)} distinct topic signals (metric: document_count) for scheduled analysis.")
        return distinct_signals
    except asyncpg.exceptions.UndefinedTableError:
        logger.error(f"Scheduler: Source signal table '{source_table}' does not exist. Cannot fetch signals.")
        return []
    except Exception as e:
        logger.error(f"Scheduler: Error fetching distinct topic signals from '{source_table}': {e}", exc_info=True)
        return []

async def get_latest_analysis_timestamp(original_signal_name: str, analysis_type_str: str) -> Optional[datetime]:
    """
    Checks the latest analysis_timestamp for a given signal and analysis type
    in the corresponding analysis_results table.
    analysis_type_str is the string value of the AnalysisType enum.
    """
    pool = await get_pool()
    results_table_name = f"{settings.ANALYSIS_RESULTS_TABLE_PREFIX}_{analysis_type_str.lower()}"
    
    try:
        async with pool.acquire() as conn:
            table_exists = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = $1);",
                results_table_name
            )
            if not table_exists:
                logger.debug(f"Analysis results table '{results_table_name}' does not exist yet. No prior analysis found.")
                return None
    except Exception as e:
        logger.error(f"Error checking existence of table '{results_table_name}': {e}")
        return None 

    query = f"""
        SELECT MAX(analysis_timestamp) 
        FROM "{results_table_name}"
        WHERE original_signal_name = $1 AND analysis_type = $2;
    """
    try:
        timestamp = await pool.fetchval(query, original_signal_name, analysis_type_str)
        if timestamp:
            logger.trace(f"Latest analysis for '{original_signal_name}' (type: {analysis_type_str}) was at {timestamp}.")
        else:
            logger.trace(f"No prior analysis found for '{original_signal_name}' (type: {analysis_type_str}) in '{results_table_name}'.")
        return timestamp
    except Exception as e:
        logger.error(f"Error fetching latest analysis timestamp for '{original_signal_name}' (type: {analysis_type_str}): {e}", exc_info=True)
        return None
# --- END NEW FUNCTIONS ---