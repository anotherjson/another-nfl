"""
Integration tests for functional CLI commands.

Tests the complete extract → process → materialize → query workflow using functional approaches.
"""

from __future__ import annotations

import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import pytest
from hypothesis import given, strategies as st

from src.functional_utils import CLIResult, ExtractionConfig, QueryParams
from src.functional_extraction import (
    extract_single_dataset,
    extract_all_datasets,
    get_extraction_status,
)
from src.functional_processing import (
    process_parquet_file,
    scan_data_directory,
    validate_all_datasets,
)
from src.functional_materialization import (
    materialize_by_priority,
    get_materialization_status,
)
from src.functional_querying import (
    query_staging_model,
    list_all_models,
    get_model_summary,
)


@pytest.fixture
def temp_data_dir():
    """Create temporary data directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_nfl_data():
    """Mock NFL data for testing."""
    return pd.DataFrame({
        'game_id': ['2023_01_BUF_MIA', '2023_01_DAL_NYG'],
        'player_id': ['12345', '67890'],
        'player_name': ['Josh Allen', 'Dak Prescott'],
        'position': ['QB', 'QB'],
        'team': ['BUF', 'DAL'],
        'season': [2023, 2023],
        'week': [1, 1],
        'completions': [20, 18],
        'attempts': [30, 25],
        'passing_yards': [280, 220]
    })


@pytest.fixture
def mock_ducklake_manager():
    """Mock DuckLakeManager for testing."""
    with patch('src.functional_querying.DuckLakeManager') as mock:
        instance = Mock()
        mock.return_value = instance
        yield instance


class TestExtractionWorkflow:
    """Test the complete extraction workflow."""
    
    @patch('src.functional_extraction.nfl')
    def test_extract_single_dataset_success(self, mock_nfl, mock_nfl_data, temp_data_dir):
        """Test successful single dataset extraction."""
        # Mock nfl_data_py
        mock_nfl.import_pbp_data.return_value = mock_nfl_data
        
        config = ExtractionConfig(
            dataset_name='pbp',
            year=2023,
            validate=True,
            save_to_disk=True,
            output_path=None
        )
        
        # Patch the base path to use temp directory
        with patch('src.functional_extraction.calculate_output_path') as mock_path:
            mock_path.return_value = temp_data_dir / 'pbp_2023_test.parquet'
            
            result = extract_single_dataset(config)
        
        assert result.success is True
        assert result.data is not None
        
        data, metadata = result.data
        assert len(data) == len(mock_nfl_data)
        assert metadata.rows_extracted == len(mock_nfl_data)
        assert metadata.validation_passed is True
    
    @patch('src.functional_extraction.nfl')
    def test_extract_single_dataset_validation_failure(self, mock_nfl):
        """Test extraction with validation failure."""
        # Mock empty DataFrame
        mock_nfl.import_pbp_data.return_value = pd.DataFrame()
        
        config = ExtractionConfig(
            dataset_name='pbp',
            year=2023,
            validate=True,
            save_to_disk=False,
            output_path=None
        )
        
        result = extract_single_dataset(config)
        
        assert result.success is False
        assert "validation failed" in result.error.lower()
    
    def test_extract_invalid_dataset(self):
        """Test extraction with invalid dataset name."""
        config = ExtractionConfig(
            dataset_name='invalid_dataset',
            year=2023,
            validate=True,
            save_to_disk=False,
            output_path=None
        )
        
        result = extract_single_dataset(config)
        
        assert result.success is False
        assert "invalid dataset" in result.error.lower()
    
    @patch('src.functional_extraction.nfl')
    def test_extract_all_datasets_partial_success(self, mock_nfl, mock_nfl_data):
        """Test extracting all datasets with some failures."""
        # Mock different datasets with different success rates
        def mock_import_func(years=None):
            if mock_import_func.call_count <= 2:  # First two calls succeed
                return mock_nfl_data
            else:  # Subsequent calls fail
                raise Exception("Network error")
        
        mock_import_func.call_count = 0
        
        def side_effect(*args, **kwargs):
            mock_import_func.call_count += 1
            return mock_import_func(*args, **kwargs)
        
        # Mock all NFL functions to use the side effect
        for attr_name in dir(mock_nfl):
            if attr_name.startswith('import_'):
                setattr(mock_nfl, attr_name, Mock(side_effect=side_effect))
        
        result = extract_all_datasets(2023, 'critical')
        
        # Should fail because some extractions failed
        assert result.success is False
        assert "failed extractions" in result.error.lower()


class TestProcessingWorkflow:
    """Test the complete processing workflow."""
    
    def test_process_parquet_file_success(self, temp_data_dir, mock_nfl_data):
        """Test successful parquet file processing."""
        # Create test parquet file
        test_file = temp_data_dir / 'test_data.parquet'
        mock_nfl_data.to_parquet(test_file)
        
        result = process_parquet_file(test_file, limit=5, validate=True)
        
        assert result.success is True
        assert result.data is not None
        
        file_data = result.data
        assert file_data['file_info']['size_mb'] > 0
        assert file_data['schema']['shape'][0] == len(mock_nfl_data)
        assert file_data['detected_type'] in ['pbp', 'weekly', 'unknown']
        assert file_data['validation_report'] is not None
    
    def test_process_nonexistent_file(self, temp_data_dir):
        """Test processing non-existent file."""
        nonexistent_file = temp_data_dir / 'nonexistent.parquet'
        
        result = process_parquet_file(nonexistent_file, limit=5, validate=False)
        
        assert result.success is False
        assert "does not exist" in result.error.lower()
    
    def test_scan_data_directory_success(self, temp_data_dir, mock_nfl_data):
        """Test successful data directory scanning."""
        # Create test directory structure
        pbp_dir = temp_data_dir / 'pbp' / '2023'
        pbp_dir.mkdir(parents=True)
        
        test_file = pbp_dir / 'pbp_2023.parquet'
        mock_nfl_data.to_parquet(test_file)
        
        result = scan_data_directory(temp_data_dir)
        
        assert result.success is True
        assert result.data is not None
        
        dataset_infos = result.data
        assert len(dataset_infos) == 1
        assert dataset_infos[0].name == 'pbp'
        assert dataset_infos[0].row_count == len(mock_nfl_data)
    
    def test_scan_empty_directory(self, temp_data_dir):
        """Test scanning empty data directory."""
        result = scan_data_directory(temp_data_dir)
        
        assert result.success is True
        assert result.data is not None
        assert len(result.data) == 0
    
    def test_validate_all_datasets_success(self, temp_data_dir, mock_nfl_data):
        """Test validation of all datasets."""
        # Create test files
        for dataset in ['pbp', 'weekly']:
            dataset_dir = temp_data_dir / dataset
            dataset_dir.mkdir()
            test_file = dataset_dir / f'{dataset}_2023.parquet'
            mock_nfl_data.to_parquet(test_file)
        
        result = validate_all_datasets(temp_data_dir)
        
        assert result.success is True
        assert result.data is not None
        
        validations = result.data
        assert len(validations) == 2
        assert all(v.validation_passed for v in validations.values())


class TestMaterializationWorkflow:
    """Test the complete materialization workflow."""
    
    @patch('src.functional_materialization.subprocess.run')
    def test_materialize_by_priority_success(self, mock_subprocess):
        """Test successful materialization by priority."""
        # Mock successful dbt run
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "model stg_pbp completed successfully\nmodel stg_weekly completed successfully"
        mock_result.stderr = ""
        mock_subprocess.return_value = mock_result
        
        result = materialize_by_priority('critical', include_tests=False)
        
        assert result.success is True
        assert result.data is not None
        
        materialization = result.data
        assert materialization.success_count >= 0
        assert materialization.error_count == 0
        assert len(materialization.models_materialized) >= 0
    
    @patch('src.functional_materialization.subprocess.run')
    def test_materialize_by_priority_failure(self, mock_subprocess):
        """Test materialization failure."""
        # Mock failed dbt run
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Error in model stg_pbp"
        mock_subprocess.return_value = mock_result
        
        result = materialize_by_priority('critical', include_tests=False)
        
        assert result.success is False
        assert "materialization failed" in result.error.lower()
    
    def test_materialize_invalid_priority(self):
        """Test materialization with invalid priority."""
        result = materialize_by_priority('invalid_priority')
        
        assert result.success is False
        assert "invalid priority" in result.error.lower()
    
    @patch('src.functional_materialization.subprocess.run')
    def test_get_materialization_status_success(self, mock_subprocess):
        """Test getting materialization status."""
        # Mock dbt list command
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "nfl_analytics.staging.stg_pbp\nnfl_analytics.staging.stg_weekly"
        mock_result.stderr = ""
        mock_subprocess.return_value = mock_result
        
        result = get_materialization_status()
        
        assert result.success is True
        assert result.data is not None
        
        status = result.data
        assert 'summary' in status
        assert 'dbt_models' in status
        assert 'dagster_assets' in status


class TestQueryWorkflow:
    """Test the complete query workflow."""
    
    def test_query_staging_model_success(self, mock_ducklake_manager, mock_nfl_data):
        """Test successful staging model query."""
        # Mock DuckLakeManager
        mock_ducklake_manager.execute_query.return_value = mock_nfl_data
        
        params = QueryParams(
            model_name='stg_pbp',
            limit=10,
            as_of_date=None,
            show_schema=False,
            filters={}
        )
        
        result = query_staging_model(params)
        
        assert result.success is True
        assert result.data is not None
        
        query_result = result.data
        assert query_result.row_count == len(mock_nfl_data)
        assert query_result.column_count == len(mock_nfl_data.columns)
        assert query_result.execution_time_ms >= 0
    
    def test_query_staging_model_invalid_params(self):
        """Test query with invalid parameters."""
        params = QueryParams(
            model_name='',  # Invalid empty model name
            limit=10,
            as_of_date=None,
            show_schema=False,
            filters={}
        )
        
        result = query_staging_model(params)
        
        assert result.success is False
        assert "model name is required" in result.error.lower()
    
    def test_query_staging_model_with_time_travel(self, mock_ducklake_manager, mock_nfl_data):
        """Test query with time travel."""
        mock_ducklake_manager.execute_query.return_value = mock_nfl_data
        
        params = QueryParams(
            model_name='stg_pbp',
            limit=5,
            as_of_date='2024-01-15',
            show_schema=False,
            filters={}
        )
        
        result = query_staging_model(params)
        
        assert result.success is True
        assert result.data is not None
        
        # Check that time travel query was constructed
        query_result = result.data
        assert "AS OF" in query_result.query_sql or "as of" in query_result.query_sql.lower()
    
    def test_list_all_models_success(self, mock_ducklake_manager):
        """Test listing all available models."""
        # Mock available models
        mock_ducklake_manager.list_available_models.return_value = [
            'stg_pbp', 'stg_weekly', 'stg_team_desc', 'int_player_stats'
        ]
        mock_ducklake_manager.execute_query.return_value = pd.DataFrame({'row_count': [1000]})
        
        result = list_all_models()
        
        assert result.success is True
        assert result.data is not None
        
        models_data = result.data
        assert models_data['total_models'] == 4
        assert len(models_data['staging_models']) == 3
        assert len(models_data['intermediate_models']) == 1
    
    def test_get_model_summary_success(self, mock_ducklake_manager, mock_nfl_data):
        """Test getting model summary."""
        # Mock schema query
        schema_df = pd.DataFrame({
            'name': ['game_id', 'player_id', 'season'],
            'type': ['VARCHAR', 'VARCHAR', 'INTEGER'],
            'notnull': [1, 1, 1],
            'dflt_value': [None, None, None],
            'pk': [0, 0, 0]
        })
        
        # Mock different queries
        def mock_execute_query(query):
            if "PRAGMA table_info" in query:
                return schema_df
            elif "COUNT(*)" in query:
                return pd.DataFrame({'row_count': [len(mock_nfl_data)]})
            else:
                return mock_nfl_data.head(5)
        
        mock_ducklake_manager.execute_query.side_effect = mock_execute_query
        
        result = get_model_summary('stg_pbp')
        
        assert result.success is True
        assert result.data is not None
        
        summary = result.data
        assert summary['model_name'] == 'stg_pbp'
        assert summary['row_count'] == len(mock_nfl_data)
        assert len(summary['schema']['columns']) == 3
        assert len(summary['sample_data']) <= 5


class TestEndToEndWorkflow:
    """Test complete end-to-end workflows."""
    
    @patch('src.functional_extraction.nfl')
    @patch('src.functional_materialization.subprocess.run')
    def test_extract_to_materialize_workflow(self, mock_subprocess, mock_nfl, 
                                           mock_nfl_data, temp_data_dir):
        """Test complete extract → materialize workflow."""
        # Mock extraction
        mock_nfl.import_pbp_data.return_value = mock_nfl_data
        
        # Mock materialization
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "model stg_pbp completed successfully"
        mock_result.stderr = ""
        mock_subprocess.return_value = mock_result
        
        # Step 1: Extract data
        config = ExtractionConfig(
            dataset_name='pbp',
            year=2023,
            validate=True,
            save_to_disk=True,
            output_path=temp_data_dir / 'pbp_2023.parquet'
        )
        
        extraction_result = extract_single_dataset(config)
        assert extraction_result.success is True
        
        # Step 2: Materialize staging models
        materialization_result = materialize_by_priority('critical', include_tests=False)
        assert materialization_result.success is True
        
        # Verify end-to-end success
        data, metadata = extraction_result.data
        materialization = materialization_result.data
        
        assert metadata.rows_extracted == len(mock_nfl_data)
        assert materialization.success_count >= 0
    
    @patch('src.functional_extraction.nfl')  
    def test_extract_to_process_workflow(self, mock_nfl, mock_nfl_data, temp_data_dir):
        """Test extract → process workflow."""
        # Mock extraction
        mock_nfl.import_pbp_data.return_value = mock_nfl_data
        
        # Step 1: Extract data
        output_file = temp_data_dir / 'pbp_2023.parquet'
        config = ExtractionConfig(
            dataset_name='pbp',
            year=2023,
            validate=True,
            save_to_disk=True,
            output_path=output_file
        )
        
        extraction_result = extract_single_dataset(config)
        assert extraction_result.success is True
        
        data, metadata = extraction_result.data
        assert metadata.output_path == output_file
        
        # Step 2: Process extracted file
        processing_result = process_parquet_file(output_file, limit=5, validate=True)
        assert processing_result.success is True
        
        file_data = processing_result.data
        assert file_data['schema']['shape'][0] == len(mock_nfl_data)
        assert file_data['validation_report'].validation_passed is True
    
    def test_process_to_query_workflow_mock(self, mock_ducklake_manager, 
                                          temp_data_dir, mock_nfl_data):
        """Test process → query workflow with mocked components."""
        # Step 1: Create processed data
        test_file = temp_data_dir / 'stg_pbp.parquet'
        mock_nfl_data.to_parquet(test_file)
        
        processing_result = process_parquet_file(test_file, limit=5, validate=True)
        assert processing_result.success is True
        
        # Step 2: Mock query of staging model
        mock_ducklake_manager.execute_query.return_value = mock_nfl_data
        
        params = QueryParams(
            model_name='stg_pbp',
            limit=10,
            as_of_date=None,
            show_schema=False,
            filters={}
        )
        
        query_result = query_staging_model(params)
        assert query_result.success is True
        
        # Verify workflow consistency
        file_data = processing_result.data
        query_data = query_result.data
        
        # Both should have data from the same source
        assert file_data['schema']['shape'][1] == query_data.column_count


class TestErrorHandlingAndRecovery:
    """Test error handling and recovery scenarios."""
    
    def test_extraction_error_propagation(self):
        """Test that extraction errors propagate correctly."""
        config = ExtractionConfig(
            dataset_name='pbp',
            year=None,  # Missing required year
            validate=True,
            save_to_disk=False,
            output_path=None
        )
        
        result = extract_single_dataset(config)
        
        assert result.success is False
        assert "requires year parameter" in result.error.lower()
    
    @patch('src.functional_materialization.subprocess.run')
    def test_materialization_timeout_handling(self, mock_subprocess):
        """Test handling of materialization timeouts."""
        # Mock timeout exception
        import subprocess
        mock_subprocess.side_effect = subprocess.TimeoutExpired('dbt', 300)
        
        result = materialize_by_priority('critical', include_tests=False)
        
        assert result.success is False
        assert "timeout" in result.error.lower()
    
    def test_query_invalid_model_handling(self, mock_ducklake_manager):
        """Test handling of queries to non-existent models."""
        # Mock DuckLakeManager to raise exception
        mock_ducklake_manager.execute_query.side_effect = Exception("Table not found")
        
        params = QueryParams(
            model_name='nonexistent_model',
            limit=10,
            as_of_date=None,
            show_schema=False,
            filters={}
        )
        
        result = query_staging_model(params)
        
        assert result.success is False
        assert "query execution failed" in result.error.lower()


class TestPerformanceAndScaling:
    """Test performance characteristics of functional operations."""
    
    @given(st.integers(min_value=1, max_value=1000))
    def test_large_dataset_processing_scalability(self, num_rows: int):
        """Test that processing scales reasonably with dataset size."""
        # Create large DataFrame
        large_data = pd.DataFrame({
            'id': range(num_rows),
            'value': [f'value_{i}' for i in range(num_rows)],
            'number': [i * 2 for i in range(num_rows)]
        })
        
        with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file:
            temp_path = Path(temp_file.name)
            large_data.to_parquet(temp_path)
            
            result = process_parquet_file(temp_path, limit=10, validate=False)
            
            assert result.success is True
            
            file_data = result.data
            assert file_data['schema']['shape'][0] == num_rows
            assert len(file_data['sample_data']) <= 10
    
    def test_multiple_result_combination_performance(self):
        """Test performance of combining many results."""
        from src.functional_utils import combine_results
        
        # Create many successful results
        results = [CLIResult.ok(i) for i in range(100)]
        
        combined = combine_results(results)
        
        assert combined.success is True
        assert len(combined.data) == 100
        assert combined.data == list(range(100))