"""
Enhanced DbtStagingConnector for direct dbt model execution with Dagster integration.

Executes dbt models via 'dbt run' and queries resulting DuckDB tables with intelligent
caching based on dbt run cycles and Dagster asset status.
"""

import streamlit as st
import duckdb
import pandas as pd
import subprocess
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Union
from dataclasses import dataclass, asdict
import time
import os

@dataclass
class ModelStatus:
    """Status information for a dbt model."""
    name: str
    last_run: Optional[datetime]
    is_fresh: bool
    row_count: Optional[int]
    error: Optional[str] = None
    dagster_asset_status: Optional[str] = None

@dataclass 
class DbtRunResult:
    """Result of a dbt run operation."""
    success: bool
    models_executed: List[str]
    execution_time: float
    error: Optional[str] = None


class DbtStagingConnector:
    """
    Enhanced connector for dbt staging models with direct execution and Dagster integration.
    
    Features:
    - Execute dbt models via subprocess
    - Intelligent caching based on dbt run frequency
    - Integration with Dagster asset status
    - Data freshness monitoring
    - Comprehensive error handling
    """
    
    def __init__(self, 
                 dbt_project_dir: str = "dbt",
                 duckdb_path: str = "data/nfl_analytics.duckdb",
                 cache_duration_minutes: int = 10):
        self.dbt_project_dir = Path(dbt_project_dir)
        self.duckdb_path = duckdb_path
        self.cache_duration = timedelta(minutes=cache_duration_minutes)
        self._connection = None
        self._model_cache = {}
        self._last_dbt_run = {}
        
        # Available staging models
        self.staging_models = [
            'stg_pbp',
            'stg_weekly', 
            'stg_team_desc',
            'stg_schedules',
            'stg_seasonal',
            'stg_players'
        ]
    
    @st.cache_resource
    def get_duckdb_connection(_self):
        """Get cached DuckDB connection to main analytics database."""
        if _self._connection is None:
            try:
                _self._connection = duckdb.connect(_self.duckdb_path)
                
                # Verify connection works
                _self._connection.execute("SELECT 1").fetchone()
                
            except Exception as e:
                st.error(f"Failed to connect to DuckDB at {_self.duckdb_path}: {e}")
                raise
                
        return _self._connection
    
    def _run_dbt_command(self, command: List[str]) -> DbtRunResult:
        """Execute a dbt command and return structured results."""
        start_time = time.time()
        
        try:
            # Change to dbt project directory
            full_command = ["uv", "run", "dbt"] + command
            
            result = subprocess.run(
                full_command,
                cwd=self.dbt_project_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                # Parse successful output for executed models
                executed_models = self._parse_dbt_output(result.stdout)
                
                return DbtRunResult(
                    success=True,
                    models_executed=executed_models,
                    execution_time=execution_time
                )
            else:
                return DbtRunResult(
                    success=False,
                    models_executed=[],
                    execution_time=execution_time,
                    error=f"dbt command failed: {result.stderr}"
                )
                
        except subprocess.TimeoutExpired:
            return DbtRunResult(
                success=False,
                models_executed=[],
                execution_time=time.time() - start_time,
                error="dbt command timed out after 5 minutes"
            )
        except Exception as e:
            return DbtRunResult(
                success=False,
                models_executed=[],
                execution_time=time.time() - start_time,
                error=f"Failed to execute dbt command: {str(e)}"
            )
    
    def _parse_dbt_output(self, output: str) -> List[str]:
        """Parse dbt output to extract executed model names."""
        executed_models = []
        lines = output.split('\n')
        
        for line in lines:
            if 'OK created' in line and 'view' in line:
                # Extract model name from lines like "OK created sql view model nfl_analytics.stg_team_desc"
                parts = line.split()
                if len(parts) >= 6:
                    model_name = parts[-1].split('.')[-1]  # Get just the model name
                    executed_models.append(model_name)
        
        return executed_models
    
    def run_staging_models(self, models: Optional[List[str]] = None) -> DbtRunResult:
        """
        Execute specific staging models or all staging models.
        
        Args:
            models: List of model names to run. If None, runs all staging models.
            
        Returns:
            DbtRunResult with execution details
        """
        if models is None:
            # Run all staging models
            command = ["run", "--select", "tag:staging"]
        else:
            # Run specific models
            model_selector = " ".join(models)
            command = ["run", "--select", model_selector]
        
        with st.spinner(f"Executing dbt models: {models or 'all staging models'}..."):
            result = self._run_dbt_command(command)
            
            if result.success:
                # Update last run times
                for model in result.models_executed:
                    self._last_dbt_run[model] = datetime.now()
                    # Invalidate cache for executed models
                    if model in self._model_cache:
                        del self._model_cache[model]
                
                st.success(f"Successfully executed {len(result.models_executed)} models in {result.execution_time:.1f}s")
            else:
                st.error(f"dbt execution failed: {result.error}")
        
        return result
    
    def get_model_status(self, model_name: str) -> ModelStatus:
        """Get comprehensive status information for a staging model."""
        try:
            conn = self.get_duckdb_connection()
            
            # Check if table exists and get row count
            try:
                row_count_result = conn.execute(f"SELECT COUNT(*) FROM {model_name}").fetchone()
                row_count = row_count_result[0] if row_count_result else 0
                table_exists = True
            except:
                row_count = None
                table_exists = False
            
            # Check last run time
            last_run = self._last_dbt_run.get(model_name)
            
            # Determine freshness (fresh if run within cache duration)
            is_fresh = (last_run is not None and 
                       datetime.now() - last_run < self.cache_duration) if last_run else False
            
            # TODO: Get Dagster asset status (will implement in next phase)
            dagster_status = "unknown"
            
            return ModelStatus(
                name=model_name,
                last_run=last_run,
                is_fresh=is_fresh and table_exists,
                row_count=row_count,
                dagster_asset_status=dagster_status
            )
            
        except Exception as e:
            return ModelStatus(
                name=model_name,
                last_run=None,
                is_fresh=False,
                row_count=None,
                error=str(e)
            )
    
    def ensure_model_fresh(self, model_name: str) -> bool:
        """
        Ensure a model is fresh, running it if necessary.
        
        Returns:
            True if model is fresh/successfully refreshed, False otherwise
        """
        status = self.get_model_status(model_name)
        
        if status.is_fresh:
            return True
        
        if status.error:
            st.warning(f"Model {model_name} has error: {status.error}")
        
        # Model is stale or doesn't exist, run it
        result = self.run_staging_models([model_name])
        
        return result.success
    
    @st.cache_data(ttl=300)  # 5 minute base cache
    def query_staging_model(_self, 
                           model_name: str, 
                           query: Optional[str] = None,
                           filters: Optional[Dict[str, Any]] = None,
                           limit: Optional[int] = None) -> pd.DataFrame:
        """
        Query a staging model with intelligent caching.
        
        Args:
            model_name: Name of the staging model to query
            query: Custom SQL query. If None, selects all from model
            filters: Dict of column: value filters to apply
            limit: Maximum number of rows to return
            
        Returns:
            DataFrame with query results
        """
        # Ensure model is fresh before querying
        if not _self.ensure_model_fresh(model_name):
            st.error(f"Failed to refresh model {model_name}")
            return pd.DataFrame()
        
        try:
            conn = _self.get_duckdb_connection()
            
            # Build query
            if query is None:
                sql_query = f"SELECT * FROM {model_name}"
                
                # Apply filters
                if filters:
                    where_clauses = []
                    for column, value in filters.items():
                        if isinstance(value, str):
                            where_clauses.append(f"{column} = '{value}'")
                        else:
                            where_clauses.append(f"{column} = {value}")
                    
                    if where_clauses:
                        sql_query += " WHERE " + " AND ".join(where_clauses)
                
                # Apply limit
                if limit:
                    sql_query += f" LIMIT {limit}"
            else:
                sql_query = query
            
            # Execute query
            result_df = conn.execute(sql_query).df()
            
            # Cache the result
            cache_key = f"{model_name}_{hash(sql_query)}"
            _self._model_cache[cache_key] = {
                'data': result_df,
                'timestamp': datetime.now()
            }
            
            return result_df
            
        except Exception as e:
            st.error(f"Failed to query model {model_name}: {e}")
            return pd.DataFrame()
    
    def get_all_model_status(self) -> Dict[str, ModelStatus]:
        """Get status for all staging models."""
        status_dict = {}
        
        for model in self.staging_models:
            status_dict[model] = self.get_model_status(model)
        
        return status_dict
    
    def refresh_all_staging_models(self) -> DbtRunResult:
        """Refresh all staging models."""
        return self.run_staging_models()
    
    def get_table_schema(self, model_name: str) -> pd.DataFrame:
        """Get schema information for a staging model."""
        try:
            conn = self.get_duckdb_connection()
            schema_df = conn.execute(f"DESCRIBE {model_name}").df()
            return schema_df
        except Exception as e:
            st.error(f"Failed to get schema for {model_name}: {e}")
            return pd.DataFrame()
    
    def close(self):
        """Close the DuckDB connection."""
        if self._connection:
            self._connection.close()
            self._connection = None


# Streamlit integration functions
@st.cache_resource
def get_dbt_connector():
    """Get cached DbtStagingConnector instance."""
    return DbtStagingConnector()


@st.cache_data(ttl=60)  # Cache for 1 minute
def get_staging_models_summary():
    """Get summary of all staging models status."""
    connector = get_dbt_connector()
    status_dict = connector.get_all_model_status()
    
    # Convert to summary format
    summary = {
        'total_models': len(status_dict),
        'fresh_models': sum(1 for s in status_dict.values() if s.is_fresh),
        'error_models': sum(1 for s in status_dict.values() if s.error),
        'total_rows': sum(s.row_count for s in status_dict.values() if s.row_count),
        'last_updated': datetime.now().isoformat(),
        'models': {name: asdict(status) for name, status in status_dict.items()}
    }
    
    return summary