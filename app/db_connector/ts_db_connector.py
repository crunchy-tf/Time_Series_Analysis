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
        # Table creation for analysis results will be on-demand or a separate setup script
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
    """
    Ensures a generic hypertable for storing analysis results exists.
    Schema might need to be more specific per analysis type or use JSONB heavily.
    """
    global _tables_checked
    if table_name in _tables_checked:
        return

    pool = await get_pool()
    logger.info(f"Checking/Creating TimescaleDB hypertable '{table_name}' for analysis results...")
    
    # Generic schema, might need adjustment
    # Consider a flexible schema using JSONB for 'result_details'
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        analysis_timestamp TIMESTAMPTZ NOT NULL,
        original_signal_name TEXT NOT NULL, -- e.g., 'agg_signals_topic_hourly.topic_123.document_count'
        analysis_type TEXT NOT NULL,        -- e.g., 'z_score', 'moving_average'
        parameters JSONB,                   -- Parameters used for the analysis
        result_value_numeric DOUBLE PRECISION, -- For single numeric results like a Z-score
        result_series_jsonb JSONB,          -- For results that are a new time series (e.g., MA)
        result_structured_jsonb JSONB,      -- For structured results like BasicStats or STL components
        metadata JSONB,                     -- Any other metadata about the analysis run
        PRIMARY KEY (original_signal_name, analysis_type, analysis_timestamp) 
        -- Add other relevant columns like topic_id if you want to query by them directly
        -- topic_id TEXT 
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
        # _tables_checked.discard(table_name) # Not strictly necessary to remove
        raise

async def fetch_time_series_data(
    signal_name: str, # This will be the table name from Signal Extraction, e.g., "agg_signals_topic_hourly"
    topic_id: Optional[str] = None, # To filter by a specific topic within that table
    metric_column: str = "document_count", # The actual value column to fetch
    start_time: datetime, 
    end_time: datetime
) -> Optional[TimeSeriesData]:
    pool = await get_pool()
    # The table name is derived from settings.SOURCE_SIGNALS_TABLE_PREFIX
    # and the type of signal (e.g., _topic_hourly)
    # For simplicity, let's assume signal_name directly maps to a table + column logic
    # A more robust mapping might be needed. Example: signal_name = "topic_123_doc_count"

    # This query needs to be adapted based on how Signal Extraction stores data.
    # Assuming Signal Extraction stores wide: (timestamp, topic_id, doc_count, sentiment_X, ...)
    source_table_name = settings.SOURCE_SIGNALS_TABLE_PREFIX + "_topic_hourly" # Example
    
    logger.info(f"Fetching data for signal '{signal_name}' (metric: {metric_column} from table {source_table_name}, topic: {topic_id}) from {start_time} to {end_time}.")

    query_conditions = ["signal_timestamp >= $1", "signal_timestamp <= $2"]
    query_params: list = [start_time, end_time]
    
    # If topic_id is part of the signal_name or passed explicitly
    actual_topic_id = topic_id
    if not actual_topic_id and "_" in signal_name: # Try to parse from signal_name like "topic_123_doc_count"
        parts = signal_name.split("_")
        if parts[0] == "topic" and len(parts) > 1: # Basic check
            actual_topic_id = parts[1] 
            # Potentially infer metric_column as well if signal_name is like "topic_123_some_metric"
            if len(parts) > 2 and metric_column == "document_count": # default metric_column
                inferred_metric_column = "_".join(parts[2:])
                if inferred_metric_column: # e.g. "dominant_sentiment_score"
                     # This requires checking if inferred_metric_column is a valid column.
                     # For now, let's assume metric_column is explicitly passed or is the default.
                     pass


    if actual_topic_id:
        query_conditions.append(f"topic_id = ${len(query_params) + 1}")
        query_params.append(actual_topic_id)

    # Ensure metric_column is a valid and safe column name
    # In a real app, validate metric_column against a list of allowed columns.
    # For now, directly embedding it but be cautious.
    if not metric_column.isalnum() and "_" not in metric_column: # Basic sanitization
        logger.error(f"Invalid metric_column provided: {metric_column}")
        return None

    query = f"""
        SELECT signal_timestamp as timestamp, {metric_column} as value 
        FROM {source_table_name}
        WHERE {" AND ".join(query_conditions)}
        ORDER BY signal_timestamp ASC;
    """
    
    points = []
    try:
        records = await pool.fetch(query, *query_params)
        for record in records:
            if record['value'] is not None: # Skip null values for analysis
                points.append(TimePoint(timestamp=record['timestamp'], value=float(record['value'])))
        
        if not points:
            logger.warning(f"No data points found for signal '{signal_name}' (metric: {metric_column}, topic: {actual_topic_id}) in the given range.")
            return None
            
        logger.info(f"Fetched {len(points)} data points for signal '{signal_name}'.")
        # Construct a more descriptive signal name for TimeSeriesData if parsed
        ts_data_signal_name = signal_name
        if actual_topic_id and metric_column != "document_count": # Be more specific
             ts_data_signal_name = f"topic_{actual_topic_id}_{metric_column}"
        elif actual_topic_id :
             ts_data_signal_name = f"topic_{actual_topic_id}_document_count"


        return TimeSeriesData(signal_name=ts_data_signal_name, points=points, metadata={"source_table": source_table_name, "metric_column": metric_column, "topic_id_filter": actual_topic_id})
    except asyncpg.exceptions.UndefinedTableError:
        logger.error(f"Source signal table '{source_table_name}' does not exist in TimescaleDB.")
        return None
    except asyncpg.exceptions.UndefinedColumnError:
        logger.error(f"Metric column '{metric_column}' or 'topic_id' does not exist in table '{source_table_name}'.")
        return None
    except Exception as e:
        logger.error(f"Error fetching time series data for '{signal_name}': {e}", exc_info=True)
        return None

async def store_analysis_result(
    original_signal_name: str, 
    analysis_type: AnalysisType, 
    parameters: Dict[str, Any],
    result_data: AnalysisResultData, # This is the Union type
    result_metadata: Optional[Dict[str, Any]] = None
):
    pool = await get_pool()
    # Example: analysis_results_zscore, analysis_results_moving_average
    table_name = f"{settings.ANALYSIS_RESULTS_TABLE_PREFIX}_{analysis_type.value.lower()}" 
    
    await _ensure_analysis_results_table_exists(table_name) # Ensure generic or specific table

    # Prepare data for insertion - this needs to be flexible based on result_data type
    # and the schema of your specific analysis results table.
    # For this generic example, we'll try to fit into the generic schema.
    
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

    # Adapt based on the actual type of result_data
    if isinstance(result_data, BasicStatsResult):
        record_to_insert["result_structured_jsonb"] = json.dumps(result_data.model_dump())
    elif isinstance(result_data, MovingAverageResult):
        record_to_insert["result_series_jsonb"] = json.dumps(result_data.moving_average_signal.model_dump())
        # May also store window and type in metadata or separate columns
    elif isinstance(result_data, ZScoreResult):
        # Storing list of ZScorePoint objects
        record_to_insert["result_structured_jsonb"] = json.dumps(
            {"points": [p.model_dump() for p in result_data.points], 
             "mean_used": result_data.mean_used, 
             "std_dev_used": result_data.std_dev_used,
             "window": result_data.window
            }
        )
    elif isinstance(result_data, STLDecompositionResult):
        # Convert datetime in original_timestamps to isoformat strings for JSON
        dumpable_stl = {
            "original_timestamps": [ts.isoformat() for ts in result_data.original_timestamps],
            "trend": result_data.trend,
            "seasonal": result_data.seasonal,
            "residual": result_data.residual,
            "period_used": result_data.period_used
        }
        record_to_insert["result_structured_jsonb"] = json.dumps(dumpable_stl)
    elif isinstance(result_data, TimeSeriesData): # For ROC, PercentChange
        record_to_insert["result_series_jsonb"] = json.dumps(result_data.model_dump())
    else:
        logger.warning(f"Unhandled result_data type for storage: {type(result_data)}")
        # Store as generic JSON if possible
        try:
            record_to_insert["result_structured_jsonb"] = json.dumps(result_data) # Fallback
        except TypeError:
            logger.error(f"Could not serialize result_data of type {type(result_data)} to JSON.")
            return


    cols = list(record_to_insert.keys())
    vals_placeholders = ", ".join([f"${i+1}" for i in range(len(cols))])
    
    # Using original_signal_name, analysis_type, analysis_timestamp as composite PK for ON CONFLICT
    # Adjust if your PK is different
    conflict_target = "(original_signal_name, analysis_type, analysis_timestamp)" 
    update_setters = ", ".join([f"{col} = EXCLUDED.{col}" for col in cols if col not in ["original_signal_name", "analysis_type", "analysis_timestamp"]])


    insert_query = f"""
        INSERT INTO {table_name} ({", ".join(cols)}) 
        VALUES ({vals_placeholders})
        ON CONFLICT {conflict_target} DO UPDATE SET {update_setters}; 
    """
    # Note: ON CONFLICT target needs to match a UNIQUE constraint or PRIMARY KEY.
    # The generic table's PK is (original_signal_name, analysis_type, analysis_timestamp)

    try:
        async with pool.acquire() as conn:
            await conn.execute(insert_query, *record_to_insert.values())
        logger.info(f"Stored/Updated analysis result for '{original_signal_name}', type '{analysis_type.value}' in '{table_name}'.")
    except Exception as e:
        logger.error(f"Error storing analysis result in '{table_name}': {e}", exc_info=True)