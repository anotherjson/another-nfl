"""Tests for CLI dbt model commands."""

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from click.testing import CliRunner

from src.cli import main
from src.ducklake_manager import DuckLakeManager


class TestCLIModels:
    """Test cases for CLI dbt model commands."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_models_help(self):
        """Test models command help."""
        result = self.runner.invoke(main, ["models", "--help"])
        assert result.exit_code == 0
        assert "Work with dbt staging models through DuckLake" in result.output

    @patch.object(DuckLakeManager, 'list_available_models')
    def test_models_list_success(self, mock_list_models):
        """Test successful models list command."""
        # Mock models data
        mock_models = [
            {
                "name": "stg_pbp",
                "description": "Staged play-by-play data",
                "schema": "nfl_raw",
                "table": "pbp"
            },
            {
                "name": "stg_weekly",
                "description": "Staged weekly player statistics",
                "schema": "nfl_raw", 
                "table": "weekly"
            }
        ]
        mock_list_models.return_value = mock_models
        
        result = self.runner.invoke(main, ["models", "list"])
        
        assert result.exit_code == 0
        assert "Available dbt Staging Models" in result.output
        assert "stg_pbp" in result.output
        assert "stg_weekly" in result.output
        assert "Staged play-by-play data" in result.output
        assert "nfl_raw" in result.output

    @patch.object(DuckLakeManager, 'list_available_models')
    def test_models_list_exception(self, mock_list_models):
        """Test models list command with exception."""
        mock_list_models.side_effect = Exception("Database connection failed")
        
        result = self.runner.invoke(main, ["models", "list"])
        
        assert result.exit_code == 1
        assert "Error listing models" in result.output
        assert "Database connection failed" in result.output

    @patch.object(DuckLakeManager, 'materialize_staging_models')
    def test_models_materialize_success(self, mock_materialize):
        """Test successful models materialize command."""
        mock_materialize.return_value = {
            "success": True,
            "stdout": "Staging models materialized successfully",
            "stderr": ""
        }
        
        result = self.runner.invoke(main, ["models", "materialize"])
        
        assert result.exit_code == 0
        assert "Triggering dbt staging model materialization" in result.output
        assert "Materialization completed successfully" in result.output

    @patch.object(DuckLakeManager, 'materialize_staging_models')
    def test_models_materialize_verbose(self, mock_materialize):
        """Test models materialize command with verbose output."""
        mock_materialize.return_value = {
            "success": True,
            "stdout": "Detailed materialization output",
            "stderr": ""
        }
        
        result = self.runner.invoke(main, ["models", "materialize", "--verbose"])
        
        assert result.exit_code == 0
        assert "Materialization completed successfully" in result.output
        assert "Detailed materialization output" in result.output

    @patch.object(DuckLakeManager, 'materialize_staging_models')
    def test_models_materialize_failure(self, mock_materialize):
        """Test failed models materialize command."""
        mock_materialize.return_value = {
            "success": False,
            "stdout": "",
            "stderr": "Materialization failed: dbt compilation error"
        }
        
        result = self.runner.invoke(main, ["models", "materialize"])
        
        assert result.exit_code == 1
        assert "Materialization failed" in result.output
        assert "dbt compilation error" in result.output

    @patch.object(DuckLakeManager, 'materialize_staging_models')
    def test_models_materialize_exception(self, mock_materialize):
        """Test models materialize command with exception."""
        mock_materialize.side_effect = Exception("Dagster not available")
        
        result = self.runner.invoke(main, ["models", "materialize"])
        
        assert result.exit_code == 1
        assert "Error triggering materialization" in result.output
        assert "Dagster not available" in result.output

    @patch.object(DuckLakeManager, 'get_model_info')
    @patch.object(DuckLakeManager, 'query_model')
    def test_models_query_success(self, mock_query, mock_get_info):
        """Test successful models query command."""
        # Mock model info
        mock_get_info.return_value = {
            "name": "stg_pbp",
            "description": "Staged play-by-play data",
            "schema": "nfl_raw",
            "table": "pbp"
        }
        
        # Mock query result
        mock_df = pd.DataFrame({
            "game_id": [2023010801, 2023010802],
            "play_id": [1, 2],
            "season": [2023, 2023]
        })
        mock_query.return_value = mock_df
        
        result = self.runner.invoke(main, ["models", "query", "stg_pbp", "--limit", "10"])
        
        assert result.exit_code == 0
        assert "Querying stg_pbp" in result.output
        assert "Results (2 rows)" in result.output
        assert "2023010801" in result.output
        mock_query.assert_called_once_with("stg_pbp", limit=10, as_of_date=None)

    @patch.object(DuckLakeManager, 'get_model_info')  
    @patch.object(DuckLakeManager, 'query_model')
    def test_models_query_with_time_travel(self, mock_query, mock_get_info):
        """Test models query command with time travel."""
        # Mock model info
        mock_get_info.return_value = {
            "name": "stg_pbp",
            "description": "Staged play-by-play data",
            "schema": "nfl_raw",
            "table": "pbp"
        }
        
        # Mock query result with time travel metadata
        mock_df = pd.DataFrame({"game_id": [2023010801]})
        mock_df.attrs = {"ducklake_version_date": "2025-01-01"}
        mock_query.return_value = mock_df
        
        result = self.runner.invoke(main, ["models", "query", "stg_pbp", "--as-of-date", "2025-01-01"])
        
        assert result.exit_code == 0
        assert "Querying stg_pbp as of 2025-01-01" in result.output
        assert "Data version used: 2025-01-01" in result.output
        mock_query.assert_called_once_with("stg_pbp", limit=10, as_of_date="2025-01-01")

    @patch.object(DuckLakeManager, 'get_model_info')
    @patch.object(DuckLakeManager, 'get_model_schema')
    @patch.object(DuckLakeManager, 'query_model')
    def test_models_query_with_schema(self, mock_query, mock_schema, mock_get_info):
        """Test models query command with schema display."""
        # Mock model info
        mock_get_info.return_value = {
            "name": "stg_pbp",
            "description": "Staged play-by-play data",
            "schema": "nfl_raw",
            "table": "pbp"
        }
        
        # Mock schema info
        mock_schema.return_value = {
            "model_name": "stg_pbp",
            "schema_name": "nfl_raw",
            "table_name": "pbp",
            "columns": [
                {"column_name": "game_id", "column_type": "BIGINT"},
                {"column_name": "play_id", "column_type": "BIGINT"}
            ]
        }
        
        # Mock query result
        mock_df = pd.DataFrame({"game_id": [2023010801], "play_id": [1]})
        mock_query.return_value = mock_df
        
        result = self.runner.invoke(main, ["models", "query", "stg_pbp", "--show-schema"])
        
        assert result.exit_code == 0
        assert "Schema for stg_pbp" in result.output
        assert "game_id" in result.output
        assert "BIGINT" in result.output

    @patch.object(DuckLakeManager, 'list_available_models')
    def test_models_query_invalid_model(self, mock_list_models):
        """Test models query command with invalid model."""
        mock_list_models.return_value = [
            {"name": "stg_pbp", "description": "Play-by-play", "schema": "nfl_raw", "table": "pbp"}
        ]
        
        with patch.object(DuckLakeManager, 'get_model_info') as mock_get_info:
            mock_get_info.side_effect = KeyError("Model not found")
            
            result = self.runner.invoke(main, ["models", "query", "invalid_model"])
            
            assert result.exit_code == 1
            assert "Invalid model: invalid_model" in result.output
            assert "Available models: stg_pbp" in result.output

    @patch.object(DuckLakeManager, 'get_model_info')
    @patch.object(DuckLakeManager, 'query_model')
    def test_models_query_empty_result(self, mock_query, mock_get_info):
        """Test models query command with empty result."""
        mock_get_info.return_value = {
            "name": "stg_pbp",
            "description": "Staged play-by-play data", 
            "schema": "nfl_raw",
            "table": "pbp"
        }
        
        mock_query.return_value = pd.DataFrame()
        
        result = self.runner.invoke(main, ["models", "query", "stg_pbp"])
        
        assert result.exit_code == 0
        assert "No data found for stg_pbp" in result.output

    @patch.object(DuckLakeManager, 'get_model_info')
    @patch.object(DuckLakeManager, 'query_model')
    def test_models_query_exception(self, mock_query, mock_get_info):
        """Test models query command with exception."""
        mock_get_info.return_value = {
            "name": "stg_pbp",
            "description": "Staged play-by-play data",
            "schema": "nfl_raw",
            "table": "pbp"
        }
        
        mock_query.side_effect = Exception("Database connection failed")
        
        result = self.runner.invoke(main, ["models", "query", "stg_pbp"])
        
        assert result.exit_code == 1
        assert "Error querying model" in result.output
        assert "Database connection failed" in result.output

    @patch.object(DuckLakeManager, 'get_catalog_tables')
    def test_models_catalog_success(self, mock_get_tables):
        """Test successful models catalog command."""
        mock_tables = [
            {
                "schema_name": "nfl_raw",
                "table_name": "pbp",
                "version_count": 2,
                "latest_etl_date": "2025-01-02",
                "total_rows": 1000,
                "created_at": pd.Timestamp("2025-01-01")
            },
            {
                "schema_name": "nfl_raw", 
                "table_name": "weekly",
                "version_count": 1,
                "latest_etl_date": "2025-01-01",
                "total_rows": 500,
                "created_at": pd.Timestamp("2025-01-01")
            }
        ]
        mock_get_tables.return_value = mock_tables
        
        result = self.runner.invoke(main, ["models", "catalog"])
        
        assert result.exit_code == 0
        assert "DuckLake Catalog Tables" in result.output
        assert "nfl_raw" in result.output
        assert "pbp" in result.output
        assert "1,000" in result.output
        assert "500" in result.output

    @patch.object(DuckLakeManager, 'get_catalog_tables')
    def test_models_catalog_empty(self, mock_get_tables):
        """Test models catalog command with no tables."""
        mock_get_tables.return_value = []
        
        result = self.runner.invoke(main, ["models", "catalog"])
        
        assert result.exit_code == 0
        assert "No tables found in DuckLake catalog" in result.output
        assert "Run 'models materialize' to create staging models" in result.output

    @patch.object(DuckLakeManager, 'get_catalog_tables')
    def test_models_catalog_exception(self, mock_get_tables):
        """Test models catalog command with exception."""
        mock_get_tables.side_effect = Exception("Catalog access failed")
        
        result = self.runner.invoke(main, ["models", "catalog"])
        
        assert result.exit_code == 1
        assert "Error accessing catalog" in result.output
        assert "Catalog access failed" in result.output

    @patch.object(DuckLakeManager, 'get_table_versions')
    def test_models_versions_success(self, mock_get_versions):
        """Test successful models versions command."""
        mock_versions = [
            {
                "version_number": 2,
                "file_path": "/data/pbp/2024/data.parquet",
                "etl_date": "2025-01-02",
                "row_count": 1000,
                "file_size": 5000000,
                "created_at": pd.Timestamp("2025-01-02 10:30:00")
            },
            {
                "version_number": 1,
                "file_path": "/data/pbp/2023/data.parquet",
                "etl_date": "2025-01-01",
                "row_count": 800,
                "file_size": 4000000,
                "created_at": pd.Timestamp("2025-01-01 09:15:00")
            }
        ]
        mock_get_versions.return_value = mock_versions
        
        result = self.runner.invoke(main, ["models", "versions", "nfl_raw.pbp"])
        
        assert result.exit_code == 0
        assert "Version History: nfl_raw.pbp" in result.output
        assert "2025-01-02" in result.output
        assert "1,000" in result.output
        assert "5.0 MB" in result.output
        mock_get_versions.assert_called_once_with("nfl_raw", "pbp")

    def test_models_versions_invalid_format(self):
        """Test models versions command with invalid table format."""
        result = self.runner.invoke(main, ["models", "versions", "pbp"])
        
        assert result.exit_code == 1
        assert "Table name must be in format 'schema.table'" in result.output
        assert "Example: nfl_raw.pbp" in result.output

    @patch.object(DuckLakeManager, 'get_table_versions')
    def test_models_versions_empty(self, mock_get_versions):
        """Test models versions command with no versions."""
        mock_get_versions.return_value = []
        
        result = self.runner.invoke(main, ["models", "versions", "nfl_raw.pbp"])
        
        assert result.exit_code == 0
        assert "No versions found for nfl_raw.pbp" in result.output

    @patch.object(DuckLakeManager, 'get_table_versions')
    def test_models_versions_exception(self, mock_get_versions):
        """Test models versions command with exception."""
        mock_get_versions.side_effect = Exception("Version access failed")
        
        result = self.runner.invoke(main, ["models", "versions", "nfl_raw.pbp"])
        
        assert result.exit_code == 1
        assert "Error getting version history" in result.output
        assert "Version access failed" in result.output

    @patch.object(DuckLakeManager, 'run_custom_query')
    def test_models_sql_success(self, mock_run_query):
        """Test successful models sql command."""
        mock_df = pd.DataFrame({
            "count": [100],
            "avg_yards": [5.2]
        })
        mock_run_query.return_value = mock_df
        
        query = "SELECT COUNT(*) as count FROM 'data.parquet'"
        result = self.runner.invoke(main, ["models", "sql", query])
        
        assert result.exit_code == 0
        assert "Executing custom SQL query" in result.output
        assert "Results (1 rows)" in result.output
        assert "100" in result.output
        mock_run_query.assert_called_once_with(query)

    @patch('pathlib.Path.exists')
    @patch('builtins.open')
    @patch.object(DuckLakeManager, 'run_custom_query')
    def test_models_sql_from_file(self, mock_run_query, mock_open, mock_exists):
        """Test models sql command reading from file."""
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value.read.return_value = "SELECT * FROM table;"
        
        mock_df = pd.DataFrame({"id": [1, 2]})
        mock_run_query.return_value = mock_df
        
        result = self.runner.invoke(main, ["models", "sql", "--file", "query.sql"])
        
        assert result.exit_code == 0
        assert "Executing custom SQL query" in result.output
        mock_run_query.assert_called_once_with("SELECT * FROM table;")

    @patch('pathlib.Path.exists')
    def test_models_sql_file_not_found(self, mock_exists):
        """Test models sql command with missing file."""
        mock_exists.return_value = False
        
        result = self.runner.invoke(main, ["models", "sql", "--file", "missing.sql"])
        
        assert result.exit_code == 1
        assert "SQL file not found: missing.sql" in result.output

    def test_models_sql_no_query(self):
        """Test models sql command without query or file."""
        result = self.runner.invoke(main, ["models", "sql"])
        
        assert result.exit_code == 1
        assert "Either provide a query or use --file" in result.output

    @patch.object(DuckLakeManager, 'run_custom_query')
    def test_models_sql_empty_result(self, mock_run_query):
        """Test models sql command with empty result."""
        mock_run_query.return_value = pd.DataFrame()
        
        result = self.runner.invoke(main, ["models", "sql", "SELECT * FROM empty_table"])
        
        assert result.exit_code == 0
        assert "Query returned no results" in result.output

    @patch.object(DuckLakeManager, 'run_custom_query')
    def test_models_sql_exception(self, mock_run_query):
        """Test models sql command with exception."""
        mock_run_query.side_effect = Exception("Invalid SQL syntax")
        
        result = self.runner.invoke(main, ["models", "sql", "INVALID SQL"])
        
        assert result.exit_code == 1
        assert "Error executing SQL query" in result.output
        assert "Invalid SQL syntax" in result.output