# Minbar Time Series Analysis Service

This microservice applies various statistical models and time series analyses to signal data. It can process data provided directly in an API request or fetch data from a TimescaleDB based on specified signal names and time ranges. The results of these analyses are then stored back into TimescaleDB.

The service includes a scheduler for automated periodic analysis of distinct signals. Additionally, a manual API endpoint is available for on-demand analysis.

## API Endpoints

---

### Time Series Analysis Endpoint

#### `POST /analyze`
-   **Description**: Performs a specified time series analysis on a given signal. The input signal data can be provided directly in the request or fetched from the database using `signal_name`, `start_time`, and `end_time`. The analysis results are returned and also stored in a dedicated analysis results table in TimescaleDB.
-   **Request Body**: `AnalysisRequest`
    -   `time_series_data` (object, optional): Directly provides the time series data for analysis. If provided, `signal_name`, `start_time`, and `end_time` should be omitted.
        -   `signal_name` (string, required within `time_series_data`): Name of the signal.
        -   `points` (list of `TimePoint`, required within `time_series_data`): List of data points, where each point has a `timestamp` (datetime string) and a `value` (float).
        -   `metadata` (object, optional): Additional metadata about the signal.
    -   `signal_name` (string, optional): Name of the signal to fetch from the database (e.g., "topic_123_document_count"). Required if `time_series_data` is not provided.
    -   `start_time` (datetime string, optional): Start time for fetching data. Required if `time_series_data` is not provided.
    -   `end_time` (datetime string, optional): End time for fetching data. Required if `time_series_data` is not provided.
    -   `analysis_type` (string, required): The type of analysis to perform. Supported values:
        -   `basic_stats`
        -   `moving_average`
        -   `rate_of_change`
        -   `percent_change`
        -   `z_score`
        -   `stl_decomposition`
    -   `parameters` (object, optional): Additional parameters specific to the chosen `analysis_type`.
        -   For `moving_average`:
            -   `window` (integer, optional, default: 7): The window size for the moving average.
            -   `type` (string, optional, default: "simple"): Type of moving average ("simple" or "exponential").
        -   For `rate_of_change` or `percent_change`:
            -   `period` (integer, optional, default: 1): The period over which to calculate the change.
        -   For `z_score`:
            -   `window` (integer, optional, default: None): Rolling window for Z-score calculation. If None, Z-score is calculated over the entire series.
        -   For `stl_decomposition`:
            -   `period` (integer, optional, default: None): The seasonal period for STL decomposition. If None, a default (e.g., 7) might be used or inferred.
            -   `robust` (boolean, optional, default: True): Whether to use a robust STL fitting.
        -   For fetching data (`signal_name` used):
            -   `metric_column_to_analyze` (string, optional, default derived from `signal_name` or "document_count"): Specifies which metric column from the source signals table to analyze (e.g., "document_count", "avg_sentiment_score_concerned").

-   **Sample Request Body (Providing data directly)**:
    ```json
    {
      "time_series_data": {
        "signal_name": "topic_123_sentiment_concerned",
        "points": [
          {"timestamp": "2024-04-15T00:00:00Z", "value": 0.5},
          {"timestamp": "2024-04-16T00:00:00Z", "value": 0.6},
          {"timestamp": "2024-04-17T00:00:00Z", "value": 0.55}
        ],
        "metadata": {"unit": "score"}
      },
      "analysis_type": "moving_average",
      "parameters": {
        "window": 2,
        "type": "simple"
      }
    }
    ```

-   **Sample Request Body (Fetching data)**:
    ```json
    {
      "signal_name": "topic_789_document_count",
      "start_time": "2024-04-01T00:00:00Z",
      "end_time": "2024-04-17T23:59:59Z",
      "analysis_type": "basic_stats",
      "parameters": {}
    }
    ```

-   **Sample Request Body (Fetching data with specific metric column)**:
    ```json
    {
      "signal_name": "topic_789", // Base signal name, metric specified in parameters
      "start_time": "2024-04-01T00:00:00Z",
      "end_time": "2024-04-17T23:59:59Z",
      "analysis_type": "rate_of_change",
      "parameters": {
        "metric_column_to_analyze": "dominant_sentiment_score",
        "period": 1
      }
    }
    ```

-   **Response Body**: `AnalysisResponse`
    -   `requested_signal_name` (string): The name of the signal that was analyzed.
    -   `analysis_type` (string): The type of analysis performed.
    -   `input_parameters` (object): The parameters used for the analysis.
    -   `result` (object): The actual result of the analysis, which varies based on `analysis_type` (e.g., `BasicStatsResult`, `MovingAverageResult`, `TimeSeriesData` for ROC).
    -   `status` (string, default: "success")
    -   `message` (string, optional)
    -   `result_metadata` (object, optional): Additional metadata about the analysis result.

---