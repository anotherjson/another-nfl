"""
Tests for the NFL Data Extractor module.
"""

import pytest
import pandas as pd
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.nfl_extractor import NFLDataExtractor
from src.config_loader import ConfigLoader


class TestNFLDataExtractor:
    """Test suite for NFLDataExtractor class."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = Path(self.temp_dir) / "configs"
        self.config_dir.mkdir()
        
        # Create a test configuration
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
        
        # Create sample test data
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        })
        
        # Mock data directory
        self.data_dir = Path(self.temp_dir) / "data"
        self.data_dir.mkdir()
        
    def teardown_method(self):
        """Clean up test fixtures after each test method."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_init_with_config_dir(self):
        """Test initialization with config directory."""
        extractor = NFLDataExtractor(self.config_dir)
        assert extractor.config_loader is not None
        # Check that the function map contains real nfl functions, not test functions
        assert 'import_pbp_data' in extractor._function_map
    
    def test_function_map_creation(self):
        """Test creation of function mapping."""
        extractor = NFLDataExtractor(self.config_dir)
        
        # Check that common functions are mapped
        expected_functions = [
            'import_pbp_data', 'import_weekly_data', 'import_seasonal_data',
            'import_team_desc', 'import_players'
        ]
        
        for func_name in expected_functions:
            assert func_name in extractor._function_map
    
    @patch('src.nfl_extractor.NFLDataExtractor._extract_with_retry')
    def test_extract_dataset_success(self, mock_extract):
        """Test successful dataset extraction."""
        mock_extract.return_value = self.sample_data
        
        extractor = NFLDataExtractor(self.config_dir)
        
        # Mock the path resolution to use temp directory
        with patch.object(extractor.config_loader, 'get_output_path') as mock_path:
            output_path = self.data_dir / "test_output.parquet"
            mock_path.return_value = str(output_path)
            
            data, metadata = extractor.extract_dataset(
                'test_dataset', year=2020, save_to_disk=False
            )
        
        assert isinstance(data, pd.DataFrame)
        assert len(data) == 3
        assert metadata['dataset'] == 'test_dataset'
        assert metadata['year'] == 2020
        assert metadata['rows_extracted'] == 3
        assert metadata['validation_passed'] is True
    
    @patch('src.nfl_extractor.NFLDataExtractor._extract_with_retry')
    def test_extract_dataset_with_save(self, mock_extract):
        """Test dataset extraction with save to disk."""
        mock_extract.return_value = self.sample_data
        
        extractor = NFLDataExtractor(self.config_dir)
        
        # Mock the path resolution
        output_path = self.data_dir / "test_output.parquet"
        with patch.object(extractor.config_loader, 'get_output_path') as mock_path:
            mock_path.return_value = str(output_path)
            
            data, metadata = extractor.extract_dataset(
                'test_dataset', year=2020, save_to_disk=True
            )
        
        assert metadata['output_path'] == str(output_path)
        assert output_path.exists()  # File should be created
        assert metadata['file_size_mb'] >= 0  # Size should be recorded (could be small)
    
    def test_extract_dataset_year_validation(self):
        """Test year validation during extraction."""
        extractor = NFLDataExtractor(self.config_dir)
        
        # Test invalid year (before start year)
        with pytest.raises(ValueError, match="Year 2019 is invalid"):
            extractor.extract_dataset('test_dataset', year=2019)
        
        # Test missing year for year-required dataset
        with pytest.raises(ValueError, match="requires a year parameter"):
            extractor.extract_dataset('test_dataset')
    
    @patch('src.nfl_extractor.nfl')
    def test_extract_with_retry_success(self, mock_nfl):
        """Test successful extraction with retry logic."""
        mock_function = MagicMock(return_value=self.sample_data)
        
        extractor = NFLDataExtractor(self.config_dir)
        extractor._function_map['import_test_data'] = mock_function
        
        config = extractor.config_loader.get_dataset_config('test_dataset')
        result = extractor._extract_with_retry(config, 2020)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        mock_function.assert_called_once_with([2020])
    
    @patch('src.nfl_extractor.nfl')
    @patch('src.nfl_extractor.time.sleep')  # Mock sleep to speed up test
    def test_extract_with_retry_failure(self, mock_sleep, mock_nfl):
        """Test extraction failure after retries."""
        mock_function = MagicMock(side_effect=Exception("API Error"))
        
        extractor = NFLDataExtractor(self.config_dir)
        extractor._function_map['import_test_data'] = mock_function
        
        config = extractor.config_loader.get_dataset_config('test_dataset')
        
        with pytest.raises(Exception, match="API Error"):
            extractor._extract_with_retry(config, 2020)
        
        # Should retry based on config (2 attempts)
        assert mock_function.call_count == 2
    
    def test_validate_data_success(self):
        """Test successful data validation."""
        extractor = NFLDataExtractor(self.config_dir)
        config = extractor.config_loader.get_dataset_config('test_dataset')
        
        result = extractor._validate_data(self.sample_data, config)
        
        assert result['passed'] is True
        assert result['row_count'] == 3
        assert result['column_count'] == 3
        assert len(result['errors']) == 0
    
    def test_validate_data_missing_columns(self):
        """Test data validation with missing required columns."""
        # Data missing required 'name' column
        invalid_data = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        })
        
        extractor = NFLDataExtractor(self.config_dir)
        config = extractor.config_loader.get_dataset_config('test_dataset')
        
        result = extractor._validate_data(invalid_data, config)
        
        assert result['passed'] is False
        assert 'name' in result['missing_columns']
        assert len(result['errors']) > 0
    
    def test_validate_data_empty_dataframe(self):
        """Test data validation with empty DataFrame."""
        empty_data = pd.DataFrame()
        
        extractor = NFLDataExtractor(self.config_dir)
        config = extractor.config_loader.get_dataset_config('test_dataset')
        
        result = extractor._validate_data(empty_data, config)
        
        assert result['passed'] is False
        assert 'Dataset is empty' in result['errors']
    
    def test_save_to_parquet(self):
        """Test saving DataFrame to parquet file."""
        extractor = NFLDataExtractor(self.config_dir)
        config = extractor.config_loader.get_dataset_config('test_dataset')
        
        # Mock path generation
        with patch.object(extractor.config_loader, 'get_output_path') as mock_path:
            output_path = self.data_dir / "test_save.parquet"
            mock_path.return_value = str(output_path)
            
            result_path = extractor._save_to_parquet(
                self.sample_data, config, year=2020, etl_date='2024-01-01'
            )
        
        assert result_path == output_path
        assert output_path.exists()
        
        # Verify we can read it back
        saved_data = pd.read_parquet(output_path)
        assert len(saved_data) == 3
        assert list(saved_data.columns) == ['id', 'name', 'value']
    
    @patch('src.nfl_extractor.NFLDataExtractor.extract_dataset')
    def test_extract_multiple_years(self, mock_extract):
        """Test extracting multiple years."""
        mock_extract.side_effect = [
            (self.sample_data, {'rows_extracted': 3, 'output_path': '/path/2020.parquet'}),
            (self.sample_data, {'rows_extracted': 3, 'output_path': '/path/2021.parquet'})
        ]
        
        extractor = NFLDataExtractor(self.config_dir)
        
        results = extractor.extract_multiple_years(
            'test_dataset', [2020, 2021], save_to_disk=False
        )
        
        assert len(results) == 2
        assert 2020 in results
        assert 2021 in results
        assert isinstance(results[2020][0], pd.DataFrame)
        assert results[2020][1]['rows_extracted'] == 3
    
    def test_extract_multiple_years_invalid_dataset(self):
        """Test extracting multiple years for non-year dataset."""
        # Create non-year dataset config
        non_year_config = self.test_config.copy()
        non_year_config['name'] = 'no_year_dataset'
        non_year_config['requires_year'] = False
        non_year_config['start_year'] = None
        
        import yaml
        config_file = self.config_dir / "no_year_dataset.yaml"
        with open(config_file, 'w') as f:
            yaml.safe_dump(non_year_config, f)
        
        extractor = NFLDataExtractor(self.config_dir)
        
        with pytest.raises(ValueError, match="doesn't support year-based extraction"):
            extractor.extract_multiple_years('no_year_dataset', [2020, 2021])
    
    def test_get_extraction_status_nonexistent(self):
        """Test getting status for non-existent extraction."""
        extractor = NFLDataExtractor(self.config_dir)
        
        with patch.object(extractor.config_loader, 'get_output_path') as mock_path:
            mock_path.return_value = str(self.data_dir / "nonexistent.parquet")
            
            status = extractor.get_extraction_status('test_dataset', year=2020)
        
        assert status['dataset'] == 'test_dataset'
        assert status['year'] == 2020
        assert status['exists'] is False
        assert status['file_info'] is None
    
    def test_get_extraction_status_existing(self):
        """Test getting status for existing extraction."""
        # Create a test parquet file
        test_file = self.data_dir / "existing.parquet"
        self.sample_data.to_parquet(test_file)
        
        extractor = NFLDataExtractor(self.config_dir)
        
        with patch.object(extractor.config_loader, 'get_output_path') as mock_path:
            mock_path.return_value = str(test_file)
            
            status = extractor.get_extraction_status('test_dataset', year=2020)
        
        assert status['exists'] is True
        assert status['file_info']['size_mb'] >= 0  # Size should be recorded (could be small)
        assert status['file_info']['rows'] == 3
    
    def test_list_available_datasets(self):
        """Test listing available datasets."""
        extractor = NFLDataExtractor(self.config_dir)
        
        datasets = extractor.list_available_datasets()
        
        assert 'test_dataset' in datasets
        assert isinstance(datasets, list)
    
    def test_get_dataset_info(self):
        """Test getting dataset information."""
        extractor = NFLDataExtractor(self.config_dir)
        
        info = extractor.get_dataset_info('test_dataset')
        
        assert info['name'] == 'test_dataset'
        assert info['description'] == 'Test dataset'
        assert info['requires_year'] is True
        assert info['start_year'] == 2020
        assert 'validation_rules' in info
        assert 'extraction_config' in info