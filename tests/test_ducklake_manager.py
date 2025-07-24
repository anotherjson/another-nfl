"""Tests for DuckLake Manager functionality."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch
import pytest
import pandas as pd
import psycopg2

from src.ducklake_manager import DuckLakeManager


class TestDuckLakeManager:
    """Test cases for DuckLake Manager."""

    def setup_method(self):
        """Set up test fixtures."""
        self.ducklake = DuckLakeManager()

    def test_init(self):
        """Test DuckLakeManager initialization."""
        manager = DuckLakeManager()
        assert manager.postgres_host == "localhost"
        assert manager.postgres_port == 5433
        assert manager.postgres_database == "nfl_ducklake"
        assert manager.duckdb_path == "data/nfl_analytics.duckdb"

    def test_list_available_models(self):
        """Test listing available dbt models."""
        models = self.ducklake.list_available_models()
        
        assert len(models) == 4
        model_names = [m["name"] for m in models]
        assert "stg_pbp" in model_names
        assert "stg_weekly" in model_names
        assert "stg_team_desc" in model_names
        assert "stg_schedules" in model_names
        
        # Check model structure
        pbp_model = next(m for m in models if m["name"] == "stg_pbp")
        assert pbp_model["description"] == "Staged play-by-play data with standardized columns"
        assert pbp_model["schema"] == "nfl_raw"
        assert pbp_model["table"] == "pbp"

    def test_get_model_info_valid(self):
        """Test getting info for valid model."""
        model_info = self.ducklake.get_model_info("stg_pbp")
        
        assert model_info["name"] == "stg_pbp"
        assert model_info["description"] == "Staged play-by-play data with standardized columns"
        assert model_info["schema"] == "nfl_raw"
        assert model_info["table"] == "pbp"

    def test_get_model_info_invalid(self):
        """Test getting info for invalid model."""
        with pytest.raises(KeyError, match="Model 'invalid_model' not found"):
            self.ducklake.get_model_info("invalid_model")

    @patch('subprocess.run')
    def test_trigger_dagster_materialization_success(self, mock_run):
        """Test successful Dagster materialization trigger."""
        # Mock successful subprocess run
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Asset materialized successfully"
        mock_result.stderr = ""
        mock_run.return_value = mock_result
        
        result = self.ducklake.trigger_dagster_materialization("test_asset")
        
        assert result["success"] is True
        assert result["stdout"] == "Asset materialized successfully"
        assert result["stderr"] == ""
        assert result["returncode"] == 0
        
        # Verify subprocess was called correctly
        mock_run.assert_called_once_with(
            ["dagster", "asset", "materialize", "--asset", "test_asset", "-f", "nfl_dagster/definitions.py"],
            capture_output=True,
            text=True,
            cwd=Path.cwd()
        )

    @patch('subprocess.run')
    def test_trigger_dagster_materialization_failure(self, mock_run):
        """Test failed Dagster materialization trigger."""
        # Mock failed subprocess run
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Asset materialization failed"
        mock_run.return_value = mock_result
        
        result = self.ducklake.trigger_dagster_materialization("test_asset")
        
        assert result["success"] is False
        assert result["stdout"] == ""
        assert result["stderr"] == "Asset materialization failed"
        assert result["returncode"] == 1

    @patch('subprocess.run')
    def test_trigger_dagster_materialization_exception(self, mock_run):
        """Test Dagster materialization trigger with exception."""
        # Mock subprocess exception
        mock_run.side_effect = Exception("Process failed")
        
        result = self.ducklake.trigger_dagster_materialization("test_asset")
        
        assert result["success"] is False
        assert "error" in result
        assert result["error"] == "Process failed"

    @patch('subprocess.run')
    def test_materialize_staging_models(self, mock_run):
        """Test materializing staging models."""
        # Mock successful subprocess run
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Staging models materialized"
        mock_result.stderr = ""
        mock_run.return_value = mock_result
        
        result = self.ducklake.materialize_staging_models()
        
        assert result["success"] is True
        mock_run.assert_called_once_with(
            ["dagster", "asset", "materialize", "--asset", "dbt_staging_models", "-f", "nfl_dagster/definitions.py"],
            capture_output=True,
            text=True,
            cwd=Path.cwd()
        )

    @patch('psycopg2.connect')
    def test_get_catalog_tables_success(self, mock_connect):
        """Test getting catalog tables successfully."""
        # Mock PostgreSQL connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock cursor description and fetchall
        mock_cursor.description = [
            ("schema_name",), ("table_name",), ("created_at",), ("updated_at",),
            ("version_count",), ("latest_etl_date",), ("total_rows",)
        ]
        mock_cursor.fetchall.return_value = [
            ("nfl_raw", "pbp", "2025-01-01", "2025-01-02", 2, "2025-01-02", 1000),
            ("nfl_raw", "weekly", "2025-01-01", "2025-01-02", 1, "2025-01-02", 500)
        ]
        
        result = self.ducklake.get_catalog_tables()
        
        assert len(result) == 2
        assert result[0]["schema_name"] == "nfl_raw"
        assert result[0]["table_name"] == "pbp"
        assert result[0]["version_count"] == 2
        assert result[0]["total_rows"] == 1000
        
        assert result[1]["schema_name"] == "nfl_raw"
        assert result[1]["table_name"] == "weekly"

    @patch('psycopg2.connect')
    def test_get_catalog_tables_exception(self, mock_connect):
        """Test getting catalog tables with exception."""
        mock_connect.side_effect = psycopg2.Error("Connection failed")
        
        with pytest.raises(RuntimeError, match="Failed to get catalog tables"):
            self.ducklake.get_catalog_tables()

    @patch('psycopg2.connect')
    def test_get_table_versions_success(self, mock_connect):
        """Test getting table versions successfully."""
        # Mock PostgreSQL connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock cursor description and fetchall
        mock_cursor.description = [
            ("version_number",), ("file_path",), ("etl_date",),
            ("row_count",), ("file_size",), ("created_at",)
        ]
        mock_cursor.fetchall.return_value = [
            (2, "/data/pbp/2024/data.parquet", "2025-01-02", 1000, 5000000, "2025-01-02"),
            (1, "/data/pbp/2023/data.parquet", "2025-01-01", 800, 4000000, "2025-01-01")
        ]
        
        result = self.ducklake.get_table_versions("nfl_raw", "pbp")
        
        assert len(result) == 2
        assert result[0]["version_number"] == 2
        assert result[0]["file_path"] == "/data/pbp/2024/data.parquet"
        assert result[0]["row_count"] == 1000
        assert result[0]["file_size"] == 5000000

    @patch('psycopg2.connect')
    def test_get_table_versions_exception(self, mock_connect):
        """Test getting table versions with exception."""
        mock_connect.side_effect = psycopg2.Error("Connection failed")
        
        with pytest.raises(RuntimeError, match="Failed to get table versions"):
            self.ducklake.get_table_versions("nfl_raw", "pbp")

    @patch.object(DuckLakeManager, 'get_postgres_connection')
    @patch.object(DuckLakeManager, 'get_duckdb_connection')
    def test_query_latest_success(self, mock_duck_conn, mock_pg_conn):
        """Test querying latest version successfully."""
        # Mock PostgreSQL connection
        mock_pg = MagicMock()
        mock_cursor = MagicMock()
        mock_pg.__enter__.return_value = mock_pg
        mock_pg.cursor.return_value.__enter__.return_value = mock_cursor
        mock_pg_conn.return_value = mock_pg
        
        mock_cursor.fetchone.return_value = ("/data/pbp/latest.parquet",)
        
        # Mock DuckDB connection
        mock_duck = MagicMock()
        mock_duck.__enter__.return_value = mock_duck
        mock_duck_conn.return_value = mock_duck
        
        # Mock DuckDB query result
        mock_df = pd.DataFrame({"game_id": [1, 2], "play_id": [1, 2]})
        mock_duck.execute.return_value.fetchdf.return_value = mock_df
        
        result = self.ducklake.query_latest("nfl_raw", "pbp", limit=10)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "game_id" in result.columns
        mock_duck.execute.assert_called_once_with("SELECT * FROM '/data/pbp/latest.parquet' LIMIT 10")

    @patch.object(DuckLakeManager, 'get_postgres_connection')
    def test_query_latest_no_data(self, mock_pg_conn):
        """Test querying latest version with no data found."""
        # Mock PostgreSQL connection returning no results
        mock_pg = MagicMock()
        mock_cursor = MagicMock()
        mock_pg.__enter__.return_value = mock_pg
        mock_pg.cursor.return_value.__enter__.return_value = mock_cursor
        mock_pg_conn.return_value = mock_pg
        
        mock_cursor.fetchone.return_value = None
        
        with pytest.raises(ValueError, match="No data found for nfl_raw.pbp"):
            self.ducklake.query_latest("nfl_raw", "pbp")

    @patch.object(DuckLakeManager, 'get_postgres_connection')
    @patch.object(DuckLakeManager, 'get_duckdb_connection')
    def test_time_travel_query_success(self, mock_duck_conn, mock_pg_conn):
        """Test time travel query successfully."""
        # Mock PostgreSQL connection
        mock_pg = MagicMock()
        mock_cursor = MagicMock()
        mock_pg.__enter__.return_value = mock_pg
        mock_pg.cursor.return_value.__enter__.return_value = mock_cursor
        mock_pg_conn.return_value = mock_pg
        
        mock_cursor.fetchone.return_value = ("/data/pbp/historical.parquet", "2025-01-01")
        
        # Mock DuckDB connection
        mock_duck = MagicMock()
        mock_duck.__enter__.return_value = mock_duck
        mock_duck_conn.return_value = mock_duck
        
        # Mock DuckDB query result
        mock_df = pd.DataFrame({"game_id": [1, 2], "play_id": [1, 2]})
        mock_duck.execute.return_value.fetchdf.return_value = mock_df
        
        result = self.ducklake.time_travel_query("nfl_raw", "pbp", "2025-01-01", limit=5)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert hasattr(result, 'attrs')
        assert result.attrs["ducklake_version_date"] == "2025-01-01"
        mock_duck.execute.assert_called_once_with("SELECT * FROM '/data/pbp/historical.parquet' LIMIT 5")

    @patch.object(DuckLakeManager, 'get_postgres_connection')
    def test_time_travel_query_no_version(self, mock_pg_conn):
        """Test time travel query with no version found."""
        # Mock PostgreSQL connection returning no results
        mock_pg = MagicMock()
        mock_cursor = MagicMock()
        mock_pg.__enter__.return_value = mock_pg
        mock_pg.cursor.return_value.__enter__.return_value = mock_cursor
        mock_pg_conn.return_value = mock_pg
        
        mock_cursor.fetchone.return_value = None
        
        with pytest.raises(ValueError, match="No version found for nfl_raw.pbp as of 2024-12-31"):
            self.ducklake.time_travel_query("nfl_raw", "pbp", "2024-12-31")

    @patch.object(DuckLakeManager, 'query_latest')
    def test_query_model_without_time_travel(self, mock_query_latest):
        """Test querying model without time travel."""
        mock_df = pd.DataFrame({"game_id": [1, 2]})
        mock_query_latest.return_value = mock_df
        
        result = self.ducklake.query_model("stg_pbp", limit=10)
        
        assert isinstance(result, pd.DataFrame)
        mock_query_latest.assert_called_once_with("nfl_raw", "pbp", 10)

    @patch.object(DuckLakeManager, 'time_travel_query')
    def test_query_model_with_time_travel(self, mock_time_travel):
        """Test querying model with time travel."""
        mock_df = pd.DataFrame({"game_id": [1, 2]})
        mock_time_travel.return_value = mock_df
        
        result = self.ducklake.query_model("stg_pbp", limit=10, as_of_date="2025-01-01")
        
        assert isinstance(result, pd.DataFrame)
        mock_time_travel.assert_called_once_with("nfl_raw", "pbp", "2025-01-01", 10)

    def test_query_model_invalid(self):
        """Test querying invalid model."""
        with pytest.raises(KeyError, match="Model 'invalid_model' not found"):
            self.ducklake.query_model("invalid_model")

    @patch.object(DuckLakeManager, 'get_duckdb_connection')
    def test_run_custom_query_success(self, mock_duck_conn):
        """Test running custom query successfully."""
        # Mock DuckDB connection
        mock_duck = MagicMock()
        mock_duck.__enter__.return_value = mock_duck
        mock_duck_conn.return_value = mock_duck
        
        # Mock query result
        mock_df = pd.DataFrame({"count": [100]})
        mock_duck.execute.return_value.fetchdf.return_value = mock_df
        
        result = self.ducklake.run_custom_query("SELECT COUNT(*) as count FROM 'data.parquet'")
        
        assert isinstance(result, pd.DataFrame)
        assert result.iloc[0]["count"] == 100
        mock_duck.execute.assert_called_once_with("SELECT COUNT(*) as count FROM 'data.parquet'")

    @patch.object(DuckLakeManager, 'get_duckdb_connection')
    def test_run_custom_query_exception(self, mock_duck_conn):
        """Test running custom query with exception."""
        mock_duck_conn.side_effect = Exception("Query failed")
        
        with pytest.raises(RuntimeError, match="Custom query failed"):
            self.ducklake.run_custom_query("INVALID SQL")

    @patch.object(DuckLakeManager, 'get_postgres_connection')
    @patch.object(DuckLakeManager, 'get_duckdb_connection')
    def test_get_model_schema_success(self, mock_duck_conn, mock_pg_conn):
        """Test getting model schema successfully."""
        # Mock PostgreSQL connection
        mock_pg = MagicMock()
        mock_cursor = MagicMock()
        mock_pg.__enter__.return_value = mock_pg
        mock_pg.cursor.return_value.__enter__.return_value = mock_cursor
        mock_pg_conn.return_value = mock_pg
        
        mock_cursor.fetchone.return_value = ("/data/pbp/latest.parquet",)
        
        # Mock DuckDB connection
        mock_duck = MagicMock()
        mock_duck.__enter__.return_value = mock_duck
        mock_duck_conn.return_value = mock_duck
        
        # Mock schema info
        schema_df = pd.DataFrame({
            "column_name": ["game_id", "play_id", "season"],
            "column_type": ["BIGINT", "BIGINT", "BIGINT"]
        })
        mock_duck.execute.return_value.fetchdf.return_value = schema_df
        
        result = self.ducklake.get_model_schema("stg_pbp")
        
        assert result["model_name"] == "stg_pbp"
        assert result["schema_name"] == "nfl_raw"
        assert result["table_name"] == "pbp"
        assert len(result["columns"]) == 3
        assert result["columns"][0]["column_name"] == "game_id"
        assert result["columns"][0]["column_type"] == "BIGINT"

    def test_get_model_schema_invalid_model(self):
        """Test getting schema for invalid model."""
        with pytest.raises(RuntimeError, match="Failed to get schema for invalid_model"):
            self.ducklake.get_model_schema("invalid_model")