"""
DuckLake Manager for CLI integration with dbt models and time travel queries.
"""

import os
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Dict, List, Tuple

import duckdb
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class DuckLakeManager:
    """Manager for DuckLake operations from CLI."""
    
    def __init__(self):
        """Initialize DuckLake manager with configuration."""
        # PostgreSQL catalog configuration
        self.postgres_host = os.getenv("POSTGRES_HOST", "localhost")
        self.postgres_port = int(os.getenv("POSTGRES_PORT", "5433"))
        self.postgres_database = os.getenv("POSTGRES_DATABASE", "nfl_ducklake")
        self.postgres_user = os.getenv("POSTGRES_USER", "nfl_user")
        self.postgres_password = os.getenv("POSTGRES_PASSWORD", "nfl_password")
        
        # DuckDB configuration
        self.duckdb_path = os.getenv("DUCKDB_DATABASE_PATH", "data/nfl_analytics.duckdb")
        self.data_path = Path(os.getenv("NFL_DATA_PATH", "data/"))
        
        # dbt configuration
        self.dbt_project_dir = Path("dbt")
        
    def get_postgres_connection(self):
        """Get PostgreSQL connection for catalog operations."""
        return psycopg2.connect(
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_database,
            user=self.postgres_user,
            password=self.postgres_password
        )
    
    def get_duckdb_connection(self):
        """Get DuckDB connection with required extensions and DuckLake attachment."""
        conn = duckdb.connect(self.duckdb_path)
        
        # Load required extensions
        try:
            conn.execute("INSTALL httpfs")
            conn.execute("INSTALL parquet")
            conn.execute("INSTALL ducklake")
            conn.execute("LOAD httpfs")
            conn.execute("LOAD parquet")
            conn.execute("LOAD ducklake")
        except Exception:
            pass  # Extensions may already be installed
        
        # Attach DuckLake
        self._ensure_ducklake_attached(conn)
        
        return conn
    
    def _ensure_ducklake_attached(self, conn):
        """Ensure DuckLake is attached to the connection."""
        try:
            ducklake_attachment = os.getenv('DUCKLAKE_ATTACHMENT')
            if ducklake_attachment:
                conn.execute(f"ATTACH '{ducklake_attachment}' AS nfl_ducklake (DATA_PATH '{self.data_path}')")
        except Exception:
            pass  # Already attached or error - handle gracefully
    
    def list_available_models(self) -> List[Dict[str, Any]]:
        """List all available dbt staging models."""
        models = [
            {
                "name": "stg_pbp",
                "description": "Staged play-by-play data with standardized columns",
                "schema": "nfl_raw",
                "table": "pbp"
            },
            {
                "name": "stg_weekly", 
                "description": "Staged weekly player statistics",
                "schema": "nfl_raw",
                "table": "weekly"
            },
            {
                "name": "stg_team_desc",
                "description": "Staged team descriptions and information", 
                "schema": "nfl_raw",
                "table": "team_desc"
            },
            {
                "name": "stg_schedules",
                "description": "Staged game schedules",
                "schema": "nfl_raw", 
                "table": "schedules"
            }
        ]
        return models
    
    def get_model_info(self, model_name: str) -> Dict[str, Any]:
        """Get information about a specific dbt model."""
        models = self.list_available_models()
        for model in models:
            if model["name"] == model_name:
                return model
        raise KeyError(f"Model '{model_name}' not found")
    
    def trigger_dagster_materialization(self, asset_name: str) -> Dict[str, Any]:
        """Trigger Dagster asset materialization."""
        try:
            # Use dagster CLI to materialize specific asset
            cmd = [
                "dagster", "asset", "materialize", 
                "--asset", asset_name,
                "-f", "nfl_dagster/definitions.py"
            ]
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True,
                cwd=Path.cwd()
            )
            
            return {
                "success": result.returncode == 0,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "returncode": result.returncode
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "stdout": "",
                "stderr": str(e)
            }
    
    def materialize_staging_models(self) -> Dict[str, Any]:
        """Materialize all dbt staging models via Dagster."""
        return self.trigger_dagster_materialization("dbt_staging_models")
    
    def get_catalog_tables(self) -> List[Dict[str, Any]]:
        """Get all tables registered in DuckLake catalog."""
        try:
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT t.schema_name, t.table_name, t.created_at, t.updated_at,
                               COUNT(tv.version_id) as version_count,
                               MAX(tv.etl_date) as latest_etl_date,
                               SUM(tv.row_count) as total_rows
                        FROM ducklake_catalog.tables t
                        LEFT JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
                        GROUP BY t.table_id, t.schema_name, t.table_name, t.created_at, t.updated_at
                        ORDER BY t.schema_name, t.table_name;
                    """)
                    
                    columns = [desc[0] for desc in cursor.description]
                    results = []
                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))
                    
                    return results
                    
        except Exception as e:
            raise RuntimeError(f"Failed to get catalog tables: {e}")
    
    def get_table_versions(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get all versions of a specific table."""
        try:
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT tv.version_number, tv.file_path, tv.etl_date,
                               tv.row_count, tv.file_size, tv.created_at
                        FROM ducklake_catalog.tables t
                        JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
                        WHERE t.schema_name = %s AND t.table_name = %s
                        ORDER BY tv.version_number DESC;
                    """, (schema_name, table_name))
                    
                    columns = [desc[0] for desc in cursor.description]
                    results = []
                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))
                    
                    return results
                    
        except Exception as e:
            raise RuntimeError(f"Failed to get table versions: {e}")
    
    def query_model(self, model_name: str, limit: Optional[int] = None, 
                   as_of_date: Optional[str] = None) -> pd.DataFrame:
        """Query a dbt model through DuckLake using direct parquet access for now."""
        
        # Map model names to their data sources temporarily
        model_data_map = {
            "stg_team_desc": "data/team_desc/etl_date=*/data.parquet",
            "stg_pbp": "data/pbp/*/etl_date=*/data.parquet", 
            "stg_weekly": "data/weekly/*/etl_date=*/data.parquet",
            "stg_schedules": "data/schedules/*/etl_date=*/data.parquet"
        }
        
        if model_name not in model_data_map:
            raise ValueError(f"Model {model_name} not found in model mapping")
        
        # Use DuckDB connection with direct parquet access
        with self.get_duckdb_connection() as conn:
            file_pattern = model_data_map[model_name]
            base_query = f"SELECT * FROM read_parquet('{file_pattern}', hive_partitioning=true, union_by_name=true)"
            
            if limit:
                base_query += f" LIMIT {limit}"
                
            return conn.execute(base_query).df()
    
    def query_latest(self, schema_name: str, table_name: str, 
                    limit: Optional[int] = None) -> pd.DataFrame:
        """Query the latest version of a table."""
        try:
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT tv.file_path
                        FROM ducklake_catalog.tables t
                        JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
                        WHERE t.schema_name = %s AND t.table_name = %s
                        ORDER BY tv.version_number DESC
                        LIMIT 1;
                    """, (schema_name, table_name))
                    
                    result = cursor.fetchone()
                    if not result:
                        raise ValueError(f"No data found for {schema_name}.{table_name}")
                    
                    file_path = result[0]
                    
                    # Query with DuckDB
                    with self.get_duckdb_connection() as duck_conn:
                        query = f"SELECT * FROM '{file_path}'"
                        if limit:
                            query += f" LIMIT {limit}"
                        return duck_conn.execute(query).fetchdf()
                        
        except Exception as e:
            raise RuntimeError(f"Failed to query {schema_name}.{table_name}: {e}")
    
    def time_travel_query(self, schema_name: str, table_name: str, 
                         as_of_date: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Query table as of specific date (time travel)."""
        try:
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT tv.file_path, tv.etl_date
                        FROM ducklake_catalog.tables t
                        JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
                        WHERE t.schema_name = %s AND t.table_name = %s 
                        AND tv.etl_date <= %s
                        ORDER BY tv.etl_date DESC
                        LIMIT 1;
                    """, (schema_name, table_name, as_of_date))
                    
                    result = cursor.fetchone()
                    if not result:
                        raise ValueError(
                            f"No version found for {schema_name}.{table_name} as of {as_of_date}"
                        )
                    
                    file_path, actual_date = result
                    
                    # Query with DuckDB
                    with self.get_duckdb_connection() as duck_conn:
                        query = f"SELECT * FROM '{file_path}'"
                        if limit:
                            query += f" LIMIT {limit}"
                        
                        df = duck_conn.execute(query).fetchdf()
                        # Add metadata about the version used
                        df.attrs["ducklake_version_date"] = actual_date
                        return df
                        
        except Exception as e:
            raise RuntimeError(f"Time travel query failed: {e}")
    
    def run_custom_query(self, query: str) -> pd.DataFrame:
        """Run a custom SQL query against DuckDB with DuckLake data."""
        try:
            with self.get_duckdb_connection() as duck_conn:
                return duck_conn.execute(query).fetchdf()
        except Exception as e:
            raise RuntimeError(f"Custom query failed: {e}")
    
    def get_model_schema(self, model_name: str) -> Dict[str, Any]:
        """Get schema information for a dbt model."""
        try:
            model_info = self.get_model_info(model_name)
            schema_name = model_info["schema"]
            table_name = model_info["table"]
            
            # Get latest file path
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT tv.file_path
                        FROM ducklake_catalog.tables t
                        JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
                        WHERE t.schema_name = %s AND t.table_name = %s
                        ORDER BY tv.version_number DESC
                        LIMIT 1;
                    """, (schema_name, table_name))
                    
                    result = cursor.fetchone()
                    if not result:
                        raise ValueError(f"No data found for {schema_name}.{table_name}")
                    
                    file_path = result[0]
                    
                    # Get schema from DuckDB
                    with self.get_duckdb_connection() as duck_conn:
                        schema_info = duck_conn.execute(f"DESCRIBE '{file_path}'").fetchdf()
                        return {
                            "model_name": model_name,
                            "schema_name": schema_name,
                            "table_name": table_name,
                            "columns": schema_info.to_dict("records")
                        }
                        
        except Exception as e:
            raise RuntimeError(f"Failed to get schema for {model_name}: {e}")
    
    def register_table(self, schema_name: str, table_name: str, file_path: str) -> None:
        """Register a new table with the DuckLake catalog."""
        try:
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cursor:
                    # First, check if table exists
                    cursor.execute("""
                        SELECT table_id FROM ducklake_catalog.tables 
                        WHERE schema_name = %s AND table_name = %s
                    """, (schema_name, table_name))
                    
                    table_result = cursor.fetchone()
                    
                    if table_result:
                        table_id = table_result[0]
                        # Get next version number
                        cursor.execute("""
                            SELECT COALESCE(MAX(version_number), 0) + 1 
                            FROM ducklake_catalog.table_versions 
                            WHERE table_id = %s
                        """, (table_id,))
                        version_number = cursor.fetchone()[0]
                    else:
                        # Create new table
                        cursor.execute("""
                            INSERT INTO ducklake_catalog.tables (schema_name, table_name, created_at, updated_at)
                            VALUES (%s, %s, NOW(), NOW())
                            RETURNING table_id
                        """, (schema_name, table_name))
                        table_id = cursor.fetchone()[0]
                        version_number = 1
                    
                    # Add new version
                    cursor.execute("""
                        INSERT INTO ducklake_catalog.table_versions 
                        (table_id, version_number, file_path, etl_date, created_at)
                        VALUES (%s, %s, %s, CURRENT_DATE, NOW())
                    """, (table_id, version_number, file_path))
                    
                    # Update table timestamp
                    cursor.execute("""
                        UPDATE ducklake_catalog.tables 
                        SET updated_at = NOW() 
                        WHERE table_id = %s
                    """, (table_id,))
                    
                    pg_conn.commit()
                    
        except Exception as e:
            raise RuntimeError(f"Failed to register table {schema_name}.{table_name}: {e}")