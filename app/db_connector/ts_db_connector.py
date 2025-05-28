# time_series_analysis_service/app/db_connector/ts_db_connector.py
import asyncpg
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from loguru import logger
import json # For storing JSONB in analysis results

from app.config import settings # Use Pydantic settings
from app.models import TimeSeriesData, TimePoint, AnalysisResultData, AnalysisType
# Import specific result models if needed for type hinting insert functions
from app.models import BasicStatsResult, MovingAverageResult, ZScoreResult, STLDecompositionResult

_pool: Optional[asyncpg.Pool] = None
_tables_checked: set = set() # To track checked/created tables

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

# --- CORRECTED PARAMETER ORDER IN THE NEXT FUNCTION ---
async def fetch_time_series_data(
    signal_name: str,
    start_time: datetime, 
    end_time: datetime,
    topic_id: Optional[str] = None, 
    metric_column: str = "document_count"
) -> Optional[TimeSeriesData]:
# --- END CORRECTION ---
    pool = await get_pool()
    source_table_name = settings.SOURCE_SIGNALS_TABLE_PREFIX + "_topic_hourly" 
    
    logger.info(f"Fetching data for signal '{signal_name}' (metric: {metric_column} from table {source_table_name}, topic: {topic_id}) from {start_time} to {end_time}.")

    query_conditions = ["signal_timestamp >= $1", "signal_timestamp <= $2"]
    query_params: list = [start_time, end_time]
    
    actual_topic_id = topic_id
    # Attempt to parse topic_id from signal_name if topic_id parameter is None
    if actual_topic_id is None and "_" in signal_name:
        parts = signal_name.split("_")
        if parts[0] == "topic" and len(parts) >= 2: # Ensure "topic" and at least an ID part
            actual_topic_id = parts[1]

    if actual_topic_id:
        query_conditions.append(f"topic_id = ${len(query_params) + 1}")
        query_params.append(str(actual_topic_id)) # Ensure topic_id is passed as string if it's TEXT in DB

    # Basic sanitization for metric_column to prevent SQL injection.
    # A whitelist approach for allowed metric_column names is more secure.
    safe_metric_column = "".join(c for c in metric_column if c.isalnum() or c == '_')
    if not safe_metric_column or safe_metric_column != metric_column:
        logger.error(f"Invalid or potentially unsafe metric_column provided: {metric_column}")
        return None

    # Quoting table and column names for safety, especially if they could be case-sensitive or contain special chars
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
            logger.warning(f"No data points found for signal '{signal_name}' (metric: {safe_metric_column}, topic: {actual_topic_id}) in the given range.")
            return None
            
        logger.info(f"Fetched {len(points)} data points for signal '{signal_name}'.")
        
        ts_data_signal_name = signal_name # Default to original request
        if actual_topic_id: # Be more specific if topic_id was used
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
        record_to_insert["result_structured_jsonb"] = json.dumps(
            {"points": [p.model_dump() for p in result_data.points], 
             "mean_used": result_data.mean_used, 
             "std_dev_used": result_data.std_dev_used,
             "window": result_data.window
            }
        )
    elif isinstance(result_data, STLDecompositionResult):
        dumpable_stl = {
            "original_timestamps": [ts.isoformat() for ts in result_data.original_timestamps],
            "trend": result_data.trend,
            "seasonal": result_data.seasonal,
            "residual": result_data.residual,
            "period_used": result_data.period_used
        }
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
    update_setters = ", ".join([f"\"{col}\" = EXCLUDED.\"{col}\"" for col in cols if col not in ["original_signal_name", "analysis_type", "analysis_timestamp"]]) # Quoted column names

    # Quoted column names in INSERT INTO as well
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