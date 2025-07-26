"""
Pure functional querying for NFL staging models via DuckLake.

All functions are pure with no side effects, returning immutable CLIResult containers.
Query pipeline: connection → query building → execution → result formatting
"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .ducklake_manager import DuckLakeManager  # Existing integration
from .functional_utils import (
    CLIResult,
    QueryParams,
    QueryResult,
    safe_call,
    compose,
    pipe,
    curry,
    freeze_dataframe,
)


# Pure connection and configuration functions
@safe_call
def get_ducklake_connection_config() -> dict[str, Any]:
    """Get DuckLake connection configuration."""
    # Read from dbt profiles or default configuration
    config = {
        "database_path": "data/nfl_analytics.duckdb",
        "catalog_connection": "postgresql://localhost:5432/ducklake_catalog",
        "schema": "nfl_raw",
        "time_travel_enabled": True
    }
    return config


@safe_call
def validate_query_params(params: QueryParams) -> QueryParams:
    """Validate query parameters."""
    if not params.model_name:
        raise ValueError("Model name is required")
    
    if params.limit is not None and params.limit <= 0:
        raise ValueError("Limit must be positive")
    
    if params.as_of_date is not None:
        # Validate date format
        try:
            datetime.fromisoformat(params.as_of_date)
        except ValueError:
            raise ValueError(f"Invalid date format: {params.as_of_date}. Use ISO format (YYYY-MM-DD)")
    
    return params


@safe_call
def get_available_models() -> list[str]:
    """Get list of available staging models."""
    try:
        manager = DuckLakeManager()
        models = manager.list_available_models()
        # Handle both list of dicts and list of strings
        if models and isinstance(models[0], dict):
            return [model["name"] for model in models if model["name"].startswith('stg_')]
        else:
            return [model for model in models if model.startswith('stg_')]
    except Exception as e:
        raise RuntimeError(f"Failed to get available models: {str(e)}")


# Pure query building functions
def build_model_query(params: QueryParams) -> str:
    """
    Pure function to build SQL query for model.
    
    Returns SQL string without executing.
    """
    model_name = params.model_name
    
    # Start with basic SELECT
    query_parts = [f"SELECT * FROM {model_name}"]
    
    # Add filters if specified
    if params.filters:
        filter_conditions = []
        for column, value in params.filters.items():
            if isinstance(value, str):
                filter_conditions.append(f"{column} = '{value}'")
            elif isinstance(value, (int, float)):
                filter_conditions.append(f"{column} = {value}")
            elif isinstance(value, list):
                # Handle IN clause
                if all(isinstance(v, str) for v in value):
                    values_str = "', '".join(value)
                    filter_conditions.append(f"{column} IN ('{values_str}')")
                else:
                    values_str = ", ".join(str(v) for v in value)
                    filter_conditions.append(f"{column} IN ({values_str})")
        
        if filter_conditions:
            query_parts.append("WHERE " + " AND ".join(filter_conditions))
    
    # Add LIMIT if specified
    if params.limit is not None:
        query_parts.append(f"LIMIT {params.limit}")
    
    return " ".join(query_parts)


def build_schema_query(model_name: str) -> str:
    """Build query to get model schema information."""
    return f"PRAGMA table_info({model_name})"


def build_count_query(model_name: str) -> str:
    """Build query to get row count for model."""
    return f"SELECT COUNT(*) as row_count FROM {model_name}"


def build_time_travel_query(params: QueryParams) -> str:
    """
    Build time travel query for DuckLake.
    
    Uses AS OF clause for historical data access.
    """
    base_query = build_model_query(
        QueryParams(
            model_name=params.model_name,
            limit=params.limit,
            as_of_date=None,  # Remove time travel from base query
            show_schema=params.show_schema,
            filters=params.filters
        )
    )
    
    if params.as_of_date:
        # Insert AS OF clause before WHERE or LIMIT
        if " WHERE " in base_query:
            base_query = base_query.replace(" WHERE ", f" AS OF '{params.as_of_date}' WHERE ")
        elif " LIMIT " in base_query:
            base_query = base_query.replace(" LIMIT ", f" AS OF '{params.as_of_date}' LIMIT ")
        else:
            base_query = base_query.replace(f" FROM {params.model_name}", 
                                          f" FROM {params.model_name} AS OF '{params.as_of_date}'")
    
    return base_query


# Pure query execution functions
@safe_call
def execute_ducklake_query(query: str, connection_config: dict[str, Any] | None = None) -> pd.DataFrame:
    """
    Pure function to execute query against DuckLake.
    
    Returns immutable DataFrame with results.
    """
    if connection_config is None:
        config_result = get_ducklake_connection_config()
        if not config_result.success:
            raise RuntimeError(f"Failed to get connection config: {config_result.error}")
        connection_config = config_result.data
    
    try:
        manager = DuckLakeManager()
        result_df = manager.run_custom_query(query)
        return freeze_dataframe(result_df)
    except Exception as e:
        raise RuntimeError(f"Query execution failed: {str(e)}")


@safe_call
def get_model_schema_info(model_name: str) -> dict[str, Any]:
    """Get schema information for a model."""
    try:
        manager = DuckLakeManager()
        # Use the get_model_schema method directly instead of building custom query
        return manager.get_model_schema(model_name)
    except Exception as e:
        raise RuntimeError(f"Failed to get schema for {model_name}: {str(e)}")


@safe_call  
def get_model_row_count(model_name: str) -> int:
    """Get row count for a model."""
    try:
        manager = DuckLakeManager()
        # Query the model directly and count rows
        data = manager.query_model(model_name)
        return len(data)
    except Exception as e:
        raise RuntimeError(f"Failed to get row count for {model_name}: {str(e)}")


# Composed query functions
def query_staging_model(params: QueryParams) -> CLIResult[QueryResult]:
    """
    Complete query pipeline for staging model.
    
    Pipeline: validate params → build query → execute → format results
    """
    def query_pipeline(validated_params: QueryParams) -> CLIResult[QueryResult]:
        start_time = time.time()
        
        try:
            manager = DuckLakeManager()
            
            # Use the manager's query_model method directly for staging models
            data = manager.query_model(
                model_name=validated_params.model_name,
                limit=validated_params.limit,
                as_of_date=validated_params.as_of_date
            )
            
            execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            # Create query result
            result = QueryResult(
                data=freeze_dataframe(data),
                row_count=len(data),
                column_count=len(data.columns),
                execution_time_ms=round(execution_time, 2),
                query_sql=f"SELECT * FROM {validated_params.model_name}" + 
                         (f" LIMIT {validated_params.limit}" if validated_params.limit else "")
            )
            
            return CLIResult.ok(result)
            
        except Exception as e:
            return CLIResult.error(f"Query execution failed: {str(e)}")
    
    return validate_query_params(params).flat_map(query_pipeline)


@curry
def query_model_with_filters(model_name: str, filters: dict[str, Any], limit: int | None = None) -> CLIResult[QueryResult]:
    """
    Curried function for querying model with filters.
    
    Enables partial application for reusable query functions.
    """
    params = QueryParams(
        model_name=model_name,
        limit=limit,
        as_of_date=None,
        show_schema=False,
        filters=filters
    )
    return query_staging_model(params)


def execute_custom_sql(sql: str) -> CLIResult[QueryResult]:
    """
    Execute custom SQL query.
    
    Returns query result with execution metadata.
    """
    if not sql.strip():
        return CLIResult.error("SQL query cannot be empty")
    
    start_time = time.time()
    
    try:
        manager = DuckLakeManager()
        data = manager.run_custom_query(sql)
        execution_time = (time.time() - start_time) * 1000
        
        result = QueryResult(
            data=freeze_dataframe(data),
            row_count=len(data),
            column_count=len(data.columns),
            execution_time_ms=round(execution_time, 2),
            query_sql=sql
        )
        
        return CLIResult.ok(result)
        
    except Exception as e:
        return CLIResult.error(f"SQL execution failed: {str(e)}")


def get_model_summary(model_name: str) -> CLIResult[dict[str, Any]]:
    """
    Get comprehensive summary of a model.
    
    Combines schema info, row count, and sample data.
    """
    # Get schema information
    schema_result = get_model_schema_info(model_name)
    if not schema_result.success:
        return CLIResult.error(f"Failed to get schema: {schema_result.error}")
    
    schema_info = schema_result.data
    
    # Get row count
    count_result = get_model_row_count(model_name)
    if not count_result.success:
        return CLIResult.error(f"Failed to get row count: {count_result.error}")
    
    row_count = count_result.data
    
    # Get sample data (first 5 rows)
    sample_params = QueryParams(
        model_name=model_name,
        limit=5,
        as_of_date=None,
        show_schema=False,
        filters={}
    )
    
    sample_result = query_staging_model(sample_params)
    if not sample_result.success:
        return CLIResult.error(f"Failed to get sample data: {sample_result.error}")
    
    sample_data = sample_result.data.data
    
    # Combine information
    summary = {
        "model_name": model_name,
        "row_count": row_count,
        "schema": schema_info,
        "sample_data": sample_data,
        "summary_timestamp": datetime.now().isoformat()
    }
    
    return CLIResult.ok(summary)


# Model discovery and catalog functions
def list_all_models() -> CLIResult[dict[str, Any]]:
    """
    List all available models with metadata.
    
    Returns comprehensive model catalog.
    """
    models_result = get_available_models()
    if not models_result.success:
        return CLIResult.error(f"Failed to get models: {models_result.error}")
    
    models = models_result.data
    
    model_catalog = {
        "total_models": len(models),
        "staging_models": [m for m in models if m.startswith('stg_')],
        "intermediate_models": [m for m in models if m.startswith('int_')],
        "mart_models": [m for m in models if m.startswith('mart_')],
        "all_models": models,
        "catalog_timestamp": datetime.now().isoformat()
    }
    
    # Add row counts for each model
    model_details = {}
    for model in models:
        count_result = get_model_row_count(model)
        if count_result.success:
            model_details[model] = {
                "row_count": count_result.data,
                "type": "staging" if model.startswith('stg_') else 
                       "intermediate" if model.startswith('int_') else
                       "mart" if model.startswith('mart_') else "other"
            }
        else:
            model_details[model] = {
                "row_count": None,
                "type": "unknown",
                "error": count_result.error
            }
    
    model_catalog["model_details"] = model_details
    
    return CLIResult.ok(model_catalog)


def search_models_by_column(column_name: str) -> CLIResult[list[str]]:
    """
    Find models that contain a specific column.
    
    Returns list of model names that have the column.
    """
    models_result = get_available_models()
    if not models_result.success:
        return CLIResult.error(f"Failed to get models: {models_result.error}")
    
    models = models_result.data
    matching_models = []
    
    for model in models:
        schema_result = get_model_schema_info(model)
        if schema_result.success and schema_result.data:
            schema_info = schema_result.data
            model_columns = [col["name"] for col in schema_info["columns"]]
            if column_name.lower() in [col.lower() for col in model_columns]:
                matching_models.append(model)
    
    return CLIResult.ok(matching_models)


# Time travel and versioning functions
def get_model_versions(model_name: str, limit: int = 10) -> CLIResult[list[dict[str, Any]]]:
    """
    Get available versions of a model for time travel.
    
    Returns list of version timestamps.
    """
    try:
        manager = DuckLakeManager()
        versions = manager.get_table_versions(model_name, limit=limit)
        
        version_list = []
        for version in versions:
            version_info = {
                "version_id": version.get("version_id"),
                "timestamp": version.get("timestamp"),
                "operation": version.get("operation", "unknown"),
                "row_count": version.get("row_count", 0)
            }
            version_list.append(version_info)
        
        return CLIResult.ok(version_list)
    
    except Exception as e:
        return CLIResult.error(f"Failed to get versions for {model_name}: {str(e)}")


def time_travel_query(model_name: str, as_of_date: str, limit: int | None = None) -> CLIResult[QueryResult]:
    """
    Execute time travel query for specific date.
    
    Wrapper around query_staging_model with time travel params.
    """
    params = QueryParams(
        model_name=model_name,
        limit=limit,
        as_of_date=as_of_date,
        show_schema=False,
        filters={}
    )
    
    return query_staging_model(params)


# Query performance and optimization functions
def analyze_query_performance(sql: str, iterations: int = 3) -> CLIResult[dict[str, Any]]:
    """
    Analyze query performance by running multiple iterations.
    
    Returns performance statistics.
    """
    if iterations <= 0:
        return CLIResult.error("Iterations must be positive")
    
    execution_times = []
    row_counts = []
    errors = []
    
    for i in range(iterations):
        result = execute_custom_sql(sql)
        
        if result.success and result.data:
            execution_times.append(result.data.execution_time_ms)
            row_counts.append(result.data.row_count)
        else:
            errors.append(f"Iteration {i+1}: {result.error}")
    
    if not execution_times:
        return CLIResult.error(f"All iterations failed: {'; '.join(errors)}")
    
    # Calculate statistics
    avg_time = sum(execution_times) / len(execution_times)
    min_time = min(execution_times)
    max_time = max(execution_times)
    
    performance_stats = {
        "sql_query": sql,
        "iterations": iterations,
        "successful_runs": len(execution_times),
        "failed_runs": len(errors),
        "avg_execution_time_ms": round(avg_time, 2),
        "min_execution_time_ms": round(min_time, 2),
        "max_execution_time_ms": round(max_time, 2),
        "row_count": row_counts[0] if row_counts else 0,
        "errors": errors,
        "analysis_timestamp": datetime.now().isoformat()
    }
    
    return CLIResult.ok(performance_stats)


# Health check functions
def check_query_system_health() -> CLIResult[dict[str, Any]]:
    """
    Check health of query system components.
    
    Tests DuckLake connectivity and model availability.
    """
    health_status = {
        "timestamp": datetime.now().isoformat(),
        "overall_healthy": True,
        "components": {}
    }
    
    # Test DuckLake connection
    try:
        config_result = get_ducklake_connection_config()
        if config_result.success:
            health_status["components"]["ducklake_config"] = {"status": "ok", "data": config_result.data}
        else:
            health_status["components"]["ducklake_config"] = {"status": "error", "error": config_result.error}
            health_status["overall_healthy"] = False
    except Exception as e:
        health_status["components"]["ducklake_config"] = {"status": "error", "error": str(e)}
        health_status["overall_healthy"] = False
    
    # Test model availability
    try:
        models_result = get_available_models()
        if models_result.success:
            models = models_result.data
            health_status["components"]["model_availability"] = {
                "status": "ok",
                "total_models": len(models),
                "staging_models": len([m for m in models if m.startswith('stg_')])
            }
        else:
            health_status["components"]["model_availability"] = {"status": "error", "error": models_result.error}
            health_status["overall_healthy"] = False
    except Exception as e:
        health_status["components"]["model_availability"] = {"status": "error", "error": str(e)}
        health_status["overall_healthy"] = False
    
    # Test basic query execution
    try:
        test_query = "SELECT 1 as test_column"
        test_result = execute_custom_sql(test_query)
        if test_result.success:
            health_status["components"]["query_execution"] = {"status": "ok", "test_query_time_ms": test_result.data.execution_time_ms}
        else:
            health_status["components"]["query_execution"] = {"status": "error", "error": test_result.error}
            health_status["overall_healthy"] = False
    except Exception as e:
        health_status["components"]["query_execution"] = {"status": "error", "error": str(e)}
        health_status["overall_healthy"] = False
    
    return CLIResult.ok(health_status)