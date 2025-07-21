"""
Tests for the configuration loader system.
"""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch

from src.config_loader import ConfigLoader, DatasetConfig


class TestConfigLoader:
    """Test suite for ConfigLoader class."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = Path(self.temp_dir) / "configs"
        self.config_dir.mkdir()
        
        # Create a sample valid configuration
        self.sample_config = {
            'name': 'test_dataset',
            'description': 'Test dataset for unit testing',
            'function_name': 'import_test_data',
            'start_year': 2020,
            'requires_year': True,
            'data_type': 'test_data',
            'partition_by': 'year',
            'validation': {
                'required_columns': ['col1', 'col2'],
                'expected_size_mb': 10
            },
            'extraction': {
                'chunk_size': 1000,
                'timeout_seconds': 120,
                'retry_attempts': 3
            },
            'output': {
                'file_format': 'parquet',
                'compression': 'snappy',
                'path_template': 'data/{dataset}/{year}/etl_date={etl_date}/data.parquet'
            }
        }
        
        # Create sample config file
        config_file = self.config_dir / "test_dataset.yaml"
        with open(config_file, 'w') as f:
            yaml.safe_dump(self.sample_config, f)
    
    def teardown_method(self):
        """Clean up test fixtures after each test method."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_init_with_valid_config_dir(self):
        """Test successful initialization with valid config directory."""
        loader = ConfigLoader(self.config_dir)
        assert len(loader._configs) == 1
        assert 'test_dataset' in loader._configs
    
    def test_init_with_nonexistent_config_dir(self):
        """Test initialization fails with non-existent config directory."""
        non_existent = Path(self.temp_dir) / "nonexistent"
        
        with pytest.raises(FileNotFoundError, match="Configuration directory not found"):
            ConfigLoader(non_existent)
    
    def test_init_with_file_instead_of_dir(self):
        """Test initialization fails when config path is a file, not directory."""
        config_file = Path(self.temp_dir) / "config.yaml"
        config_file.touch()
        
        with pytest.raises(NotADirectoryError, match="Config path is not a directory"):
            ConfigLoader(config_file)
    
    def test_init_with_empty_config_dir(self):
        """Test initialization fails with empty config directory."""
        empty_dir = Path(self.temp_dir) / "empty"
        empty_dir.mkdir()
        
        with pytest.raises(ValueError, match="No YAML configuration files found"):
            ConfigLoader(empty_dir)
    
    def test_load_config_missing_required_fields(self):
        """Test loading fails with missing required fields."""
        # Create config missing required field
        invalid_config = self.sample_config.copy()
        del invalid_config['function_name']
        
        config_file = self.config_dir / "invalid.yaml"
        with open(config_file, 'w') as f:
            yaml.safe_dump(invalid_config, f)
        
        with pytest.raises(Exception, match="Missing required fields"):
            ConfigLoader(self.config_dir)
    
    def test_load_config_invalid_partition_by(self):
        """Test loading fails with invalid partition_by value."""
        invalid_config = self.sample_config.copy()
        invalid_config['partition_by'] = 'invalid_partition'
        
        config_file = self.config_dir / "invalid.yaml"
        with open(config_file, 'w') as f:
            yaml.safe_dump(invalid_config, f)
        
        with pytest.raises(Exception, match="Invalid partition_by"):
            ConfigLoader(self.config_dir)
    
    def test_load_config_requires_year_without_start_year(self):
        """Test loading fails when requires_year is True but start_year is None."""
        invalid_config = self.sample_config.copy()
        invalid_config['requires_year'] = True
        invalid_config['start_year'] = None
        
        config_file = self.config_dir / "invalid.yaml"
        with open(config_file, 'w') as f:
            yaml.safe_dump(invalid_config, f)
        
        with pytest.raises(Exception, match="If requires_year is True, start_year must be specified"):
            ConfigLoader(self.config_dir)
    
    def test_get_dataset_config_valid(self):
        """Test getting configuration for valid dataset."""
        loader = ConfigLoader(self.config_dir)
        config = loader.get_dataset_config('test_dataset')
        
        assert isinstance(config, DatasetConfig)
        assert config.name == 'test_dataset'
        assert config.function_name == 'import_test_data'
        assert config.start_year == 2020
    
    def test_get_dataset_config_invalid(self):
        """Test getting configuration for non-existent dataset."""
        loader = ConfigLoader(self.config_dir)
        
        with pytest.raises(KeyError, match="Dataset 'nonexistent' not found"):
            loader.get_dataset_config('nonexistent')
    
    def test_list_datasets(self):
        """Test listing all available datasets."""
        loader = ConfigLoader(self.config_dir)
        datasets = loader.list_datasets()
        
        assert datasets == ['test_dataset']
        assert isinstance(datasets, list)
    
    def test_get_datasets_by_type(self):
        """Test getting datasets by data type."""
        loader = ConfigLoader(self.config_dir)
        datasets = loader.get_datasets_by_type('test_data')
        
        assert datasets == ['test_dataset']
    
    def test_get_datasets_requiring_year(self):
        """Test getting datasets that require year parameter."""
        loader = ConfigLoader(self.config_dir)
        datasets = loader.get_datasets_requiring_year()
        
        assert datasets == ['test_dataset']
    
    def test_get_datasets_by_start_year(self):
        """Test getting datasets available from specific year."""
        loader = ConfigLoader(self.config_dir)
        
        # Dataset starts in 2020, so should be available for 2020+
        datasets_2020 = loader.get_datasets_by_start_year(2020)
        assert 'test_dataset' in datasets_2020
        
        # Should not be available for years before 2020
        datasets_2019 = loader.get_datasets_by_start_year(2019)
        assert 'test_dataset' not in datasets_2019
    
    def test_validate_year_for_dataset_valid(self):
        """Test year validation for valid year."""
        loader = ConfigLoader(self.config_dir)
        
        # Year 2020 and later should be valid
        assert loader.validate_year_for_dataset('test_dataset', 2020) is True
        assert loader.validate_year_for_dataset('test_dataset', 2023) is True
    
    def test_validate_year_for_dataset_invalid(self):
        """Test year validation for invalid year."""
        loader = ConfigLoader(self.config_dir)
        
        # Year before start_year should be invalid
        with pytest.raises(ValueError, match="Year 2019 is invalid for dataset"):
            loader.validate_year_for_dataset('test_dataset', 2019)
    
    def test_validate_year_for_dataset_no_year_required(self):
        """Test year validation for dataset that doesn't require year."""
        # Create config that doesn't require year
        no_year_config = self.sample_config.copy()
        no_year_config['name'] = 'no_year_dataset'
        no_year_config['requires_year'] = False
        no_year_config['start_year'] = None
        no_year_config['partition_by'] = 'etl_date_only'
        no_year_config['output']['path_template'] = 'data/{dataset}/etl_date={etl_date}/data.parquet'
        
        config_file = self.config_dir / "no_year.yaml"
        with open(config_file, 'w') as f:
            yaml.safe_dump(no_year_config, f)
        
        loader = ConfigLoader(self.config_dir)
        
        # Any year should be valid for datasets that don't require year
        assert loader.validate_year_for_dataset('no_year_dataset', 1990) is True
    
    def test_get_output_path_with_year(self):
        """Test output path generation with year parameter."""
        loader = ConfigLoader(self.config_dir)
        path = loader.get_output_path('test_dataset', year=2023, etl_date='2024-01-15')
        
        expected = 'data/test_dataset/2023/etl_date=2024-01-15/data.parquet'
        assert path == expected
    
    def test_get_output_path_with_custom_etl_date(self):
        """Test output path generation with custom ETL date."""
        loader = ConfigLoader(self.config_dir)
        path = loader.get_output_path('test_dataset', year=2023, etl_date='2024-01-01')
        
        expected = 'data/test_dataset/2023/etl_date=2024-01-01/data.parquet'
        assert path == expected
    
    def test_get_output_path_no_year_dataset(self):
        """Test output path generation for dataset that doesn't require year."""
        # Create config that doesn't require year
        no_year_config = self.sample_config.copy()
        no_year_config['name'] = 'no_year_dataset'
        no_year_config['requires_year'] = False
        no_year_config['start_year'] = None
        no_year_config['partition_by'] = 'etl_date_only'
        no_year_config['output']['path_template'] = 'data/{dataset}/etl_date={etl_date}/data.parquet'
        
        config_file = self.config_dir / "no_year.yaml"
        with open(config_file, 'w') as f:
            yaml.safe_dump(no_year_config, f)
        
        loader = ConfigLoader(self.config_dir)
        path = loader.get_output_path('no_year_dataset', etl_date='2024-01-01')
        
        expected = 'data/no_year_dataset/etl_date=2024-01-01/data.parquet'
        assert path == expected
    
    def test_multiple_config_files(self):
        """Test loading multiple configuration files."""
        # Create second config file
        second_config = self.sample_config.copy()
        second_config['name'] = 'second_dataset'
        second_config['function_name'] = 'import_second_data'
        
        config_file = self.config_dir / "second_dataset.yaml"
        with open(config_file, 'w') as f:
            yaml.safe_dump(second_config, f)
        
        loader = ConfigLoader(self.config_dir)
        
        assert len(loader._configs) == 2
        assert 'test_dataset' in loader._configs
        assert 'second_dataset' in loader._configs
        
        datasets = loader.list_datasets()
        assert sorted(datasets) == ['second_dataset', 'test_dataset']
    
    def test_invalid_yaml_syntax(self):
        """Test handling of invalid YAML syntax."""
        # Create file with invalid YAML
        invalid_file = self.config_dir / "invalid.yaml"
        with open(invalid_file, 'w') as f:
            f.write("invalid: yaml: syntax:")
        
        with pytest.raises(Exception, match="Failed to parse YAML file"):
            ConfigLoader(self.config_dir)