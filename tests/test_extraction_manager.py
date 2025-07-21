"""
Tests for the Extraction Manager module.
"""

import pytest
import pandas as pd
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from src.extraction_manager import ExtractionManager


class TestExtractionManager:
    """Test suite for ExtractionManager class."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = Path(self.temp_dir) / "configs"
        self.config_dir.mkdir()
        self.state_file = Path(self.temp_dir) / "state.json"
        
        # Create test configuration
        self.test_config = {
            'name': 'test_dataset',
            'description': 'Test dataset',
            'function_name': 'import_test_data',
            'start_year': 2020,
            'requires_year': True,
            'data_type': 'test',
            'partition_by': 'year',
            'validation': {
                'required_columns': ['id', 'name'],
                'expected_size_mb': 1
            },
            'extraction': {
                'timeout_seconds': 60,
                'retry_attempts': 2
            },
            'output': {
                'file_format': 'parquet',
                'compression': 'snappy',
                'path_template': 'data/{dataset}/{year}/etl_date={etl_date}/data.parquet'
            }
        }
        
        # Save test config
        import yaml
        config_file = self.config_dir / "test_dataset.yaml"
        with open(config_file, 'w') as f:
            yaml.safe_dump(self.test_config, f)
        
        # Sample data
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
    
    def teardown_method(self):
        """Clean up test fixtures after each test method."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_init_with_new_state_file(self):
        """Test initialization with new state file."""
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        assert manager.state_file == self.state_file
        assert 'extractions' in manager.state
        assert 'metadata' in manager.state
        assert manager.state['extractions'] == {}
    
    def test_init_with_existing_state_file(self):
        """Test initialization with existing state file."""
        # Create existing state
        existing_state = {
            'last_updated': '2024-01-01T00:00:00',
            'extractions': {
                'test_dataset': {
                    '2020': {
                        'status': 'completed',
                        'extraction_date': '2024-01-01T00:00:00'
                    }
                }
            },
            'metadata': {'version': '1.0'}
        }
        
        with open(self.state_file, 'w') as f:
            json.dump(existing_state, f)
        
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        assert 'test_dataset' in manager.state['extractions']
        assert '2020' in manager.state['extractions']['test_dataset']
    
    def test_get_missing_extractions_empty_state(self):
        """Test getting missing extractions with empty state."""
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        missing = manager.get_missing_extractions('test_dataset', [2020, 2021, 2022])
        
        assert missing == [2020, 2021, 2022]
    
    def test_get_missing_extractions_with_existing(self):
        """Test getting missing extractions with some existing."""
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        # Simulate existing extraction
        manager.state['extractions']['test_dataset'] = {
            '2020': {'status': 'completed'},
            '2021': {'status': 'failed'}  # Failed extractions should be re-extracted
        }
        
        missing = manager.get_missing_extractions('test_dataset', [2020, 2021, 2022])
        
        # Only 2020 is completed, so 2021 and 2022 are missing
        assert set(missing) == {2021, 2022}
    
    def test_is_extraction_current_true(self):
        """Test checking if extraction is current - true case."""
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        # Add recent extraction
        recent_date = datetime.now().isoformat()
        manager.state['extractions']['test_dataset'] = {
            '2020': {
                'status': 'completed',
                'extraction_date': recent_date
            }
        }
        
        is_current = manager.is_extraction_current('test_dataset', 2020, max_age_days=1)
        
        assert is_current is True
    
    def test_is_extraction_current_false_old(self):
        """Test checking if extraction is current - false due to age."""
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        # Add old extraction
        old_date = (datetime.now() - timedelta(days=5)).isoformat()
        manager.state['extractions']['test_dataset'] = {
            '2020': {
                'status': 'completed',
                'extraction_date': old_date
            }
        }
        
        is_current = manager.is_extraction_current('test_dataset', 2020, max_age_days=1)
        
        assert is_current is False
    
    def test_is_extraction_current_false_failed(self):
        """Test checking if extraction is current - false due to failed status."""
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        # Add failed extraction
        recent_date = datetime.now().isoformat()
        manager.state['extractions']['test_dataset'] = {
            '2020': {
                'status': 'failed',
                'extraction_date': recent_date
            }
        }
        
        is_current = manager.is_extraction_current('test_dataset', 2020, max_age_days=1)
        
        assert is_current is False
    
    @patch('src.extraction_manager.NFLDataExtractor.extract_dataset')
    def test_extract_incremental_success(self, mock_extract):
        """Test successful incremental extraction."""
        mock_extract.return_value = (
            self.sample_data,
            {
                'rows_extracted': 3,
                'output_path': '/path/to/file.parquet',
                'validation_passed': True
            }
        )
        
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        summary = manager.extract_incremental('test_dataset', [2020, 2021])
        
        assert summary['success'] is True
        assert summary['years_extracted'] == [2020, 2021]
        assert summary['total_rows'] == 6  # 3 rows * 2 years
        assert summary['total_files'] == 2
        assert len(summary['errors']) == 0
        
        # Check state was updated
        assert 'test_dataset' in manager.state['extractions']
        assert '2020' in manager.state['extractions']['test_dataset']
        assert manager.state['extractions']['test_dataset']['2020']['status'] == 'completed'
    
    @patch('src.extraction_manager.NFLDataExtractor.extract_dataset')
    def test_extract_incremental_with_skips(self, mock_extract):
        """Test incremental extraction with some years skipped."""
        # Setup existing extraction
        manager = ExtractionManager(self.config_dir, self.state_file)
        recent_date = datetime.now().isoformat()
        manager.state['extractions']['test_dataset'] = {
            '2020': {
                'status': 'completed',
                'extraction_date': recent_date
            }
        }
        
        mock_extract.return_value = (
            self.sample_data,
            {'rows_extracted': 3, 'output_path': '/path/file.parquet'}
        )
        
        summary = manager.extract_incremental('test_dataset', [2020, 2021])
        
        assert 2020 in summary['years_skipped']  # Should be skipped (current)
        assert summary['years_extracted'] == [2021]  # Only 2021 extracted
    
    @patch('src.extraction_manager.NFLDataExtractor.extract_dataset')
    def test_extract_incremental_non_year_dataset(self, mock_extract):
        """Test incremental extraction for non-year dataset."""
        # Create non-year config
        non_year_config = self.test_config.copy()
        non_year_config['name'] = 'no_year_dataset'
        non_year_config['requires_year'] = False
        
        import yaml
        config_file = self.config_dir / "no_year_dataset.yaml"
        with open(config_file, 'w') as f:
            yaml.safe_dump(non_year_config, f)
        
        mock_extract.return_value = (
            self.sample_data,
            {'rows_extracted': 3, 'output_path': '/path/file.parquet'}
        )
        
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        summary = manager.extract_incremental('no_year_dataset')
        
        assert summary['success'] is True
        assert summary['years_extracted'] == ['no_year']
        assert summary['total_rows'] == 3
    
    @patch('src.extraction_manager.NFLDataExtractor.extract_dataset')
    def test_extract_incremental_with_errors(self, mock_extract):
        """Test incremental extraction with some errors."""
        mock_extract.side_effect = [
            Exception("Network error"),  # 2020 fails
            (self.sample_data, {'rows_extracted': 3, 'output_path': '/path/file.parquet'})  # 2021 succeeds
        ]
        
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        summary = manager.extract_incremental('test_dataset', [2020, 2021])
        
        assert summary['success'] is False  # Has errors
        assert len(summary['errors']) == 1
        assert 'Network error' in summary['errors'][0]
        assert summary['years_extracted'] == [2021]  # 2021 succeeded
        
        # Check state reflects both success and failure
        assert manager.state['extractions']['test_dataset']['2020']['status'] == 'failed'
        assert manager.state['extractions']['test_dataset']['2021']['status'] == 'completed'
    
    def test_record_extraction(self):
        """Test recording extraction in state."""
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        metadata = {'rows_extracted': 100, 'file_path': '/path/file.parquet'}
        manager._record_extraction('test_dataset', 2020, metadata, 'completed')
        
        assert 'test_dataset' in manager.state['extractions']
        assert '2020' in manager.state['extractions']['test_dataset']
        extraction_record = manager.state['extractions']['test_dataset']['2020']
        assert extraction_record['status'] == 'completed'
        assert extraction_record['metadata'] == metadata
        
        # Check state file was saved
        assert self.state_file.exists()
    
    def test_get_extraction_summary_single_dataset(self):
        """Test getting extraction summary for single dataset."""
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        # Add some test data
        manager.state['extractions']['test_dataset'] = {
            '2020': {
                'status': 'completed',
                'extraction_date': '2024-01-01T00:00:00',
                'metadata': {'rows_extracted': 100}
            },
            '2021': {
                'status': 'failed',
                'extraction_date': '2024-01-02T00:00:00',
                'metadata': {'error': 'Network timeout'}
            }
        }
        
        summary = manager.get_extraction_summary('test_dataset')
        
        assert summary['total_datasets'] == 1
        assert summary['total_extractions'] == 2
        assert summary['successful_extractions'] == 1
        assert summary['failed_extractions'] == 1
        assert 'test_dataset' in summary['datasets']
        
        ds_summary = summary['datasets']['test_dataset']
        assert ds_summary['total_years'] == 2
        assert ds_summary['completed'] == 1
        assert ds_summary['failed'] == 1
    
    def test_get_extraction_summary_all_datasets(self):
        """Test getting extraction summary for all datasets."""
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        # Add test data for multiple datasets
        manager.state['extractions'] = {
            'dataset1': {
                '2020': {
                    'status': 'completed',
                    'extraction_date': '2024-01-01T00:00:00',
                    'metadata': {'rows_extracted': 100}
                }
            },
            'dataset2': {
                '2021': {
                    'status': 'failed',
                    'extraction_date': '2024-01-02T00:00:00',
                    'metadata': {'error': 'API error'}
                }
            }
        }
        
        summary = manager.get_extraction_summary()
        
        assert summary['total_datasets'] == 2
        assert summary['total_extractions'] == 2
        assert summary['successful_extractions'] == 1
        assert summary['failed_extractions'] == 1
        assert 'dataset1' in summary['datasets']
        assert 'dataset2' in summary['datasets']
    
    def test_cleanup_old_extractions(self):
        """Test cleanup of old extractions."""
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        # Create test files
        old_file = Path(self.temp_dir) / "old_file.parquet"
        old_file.touch()
        
        # Add old extraction to state
        old_date = (datetime.now() - timedelta(days=40)).isoformat()
        manager.state['extractions']['test_dataset'] = {
            '2020': {
                'status': 'completed',
                'extraction_date': old_date,
                'metadata': {'output_path': str(old_file)}
            }
        }
        
        cleanup_summary = manager.cleanup_old_extractions(max_age_days=30)
        
        assert cleanup_summary['files_deleted'] == 1
        assert cleanup_summary['state_entries_cleaned'] == 1
        assert not old_file.exists()  # File should be deleted
        assert 'test_dataset' not in manager.state['extractions']  # Empty dataset removed
    
    def test_get_recommended_years(self):
        """Test getting recommended years for extraction."""
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        recommended = manager.get_recommended_years('test_dataset', limit=3)
        
        # Should return recent years (working backwards from last year)
        current_year = datetime.now().year
        expected_years = [current_year - 1, current_year - 2, current_year - 3]
        
        assert recommended == expected_years
    
    def test_get_recommended_years_non_year_dataset(self):
        """Test getting recommended years for non-year dataset."""
        # Create non-year config
        non_year_config = self.test_config.copy()
        non_year_config['name'] = 'no_year_dataset'
        non_year_config['requires_year'] = False
        
        import yaml
        config_file = self.config_dir / "no_year_dataset.yaml"
        with open(config_file, 'w') as f:
            yaml.safe_dump(non_year_config, f)
        
        manager = ExtractionManager(self.config_dir, self.state_file)
        
        recommended = manager.get_recommended_years('no_year_dataset')
        
        assert recommended == []  # Non-year datasets return empty list