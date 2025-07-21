"""
Tests for the CLI extract commands.
"""

import pytest
import pandas as pd
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from src.cli import main


class TestCLIExtract:
    """Test suite for CLI extract commands."""
    
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.runner = CliRunner()
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        })
        
        self.sample_metadata = {
            'dataset': 'test_dataset',
            'year': 2020,
            'rows_extracted': 3,
            'duration_seconds': 1.5,
            'validation_passed': True,
            'output_path': '/path/to/data.parquet',
            'file_size_mb': 0.1
        }
    
    @patch('src.cli.NFLDataExtractor')
    def test_extract_dataset_success(self, mock_extractor_class):
        """Test successful dataset extraction."""
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor
        
        mock_extractor.get_dataset_info.return_value = {
            'name': 'pbp',
            'requires_year': True,
            'start_year': 1999
        }
        
        mock_extractor.extract_dataset.return_value = (self.sample_data, self.sample_metadata)
        
        result = self.runner.invoke(main, ['extract', 'dataset', 'pbp', '--year', '2020'])
        
        assert result.exit_code == 0
        assert 'Extraction completed successfully' in result.output
        assert 'Rows Extracted' in result.output
        assert '3' in result.output
        
        mock_extractor.extract_dataset.assert_called_once_with(
            dataset_name='pbp',
            year=2020,
            validate=True,
            save_to_disk=True
        )
    
    @patch('src.cli.NFLDataExtractor')
    def test_extract_dataset_invalid_dataset(self, mock_extractor_class):
        """Test extraction with invalid dataset name."""
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor
        
        mock_extractor.get_dataset_info.side_effect = KeyError('Dataset not found')
        mock_extractor.list_available_datasets.return_value = ['pbp', 'weekly', 'seasonal']
        
        result = self.runner.invoke(main, ['extract', 'dataset', 'invalid_dataset', '--year', '2020'])
        
        assert result.exit_code == 1
        assert 'Invalid dataset: invalid_dataset' in result.output
        assert 'Available datasets: pbp, weekly, seasonal' in result.output
    
    @patch('src.cli.NFLDataExtractor')
    def test_extract_dataset_missing_year(self, mock_extractor_class):
        """Test extraction with missing required year."""
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor
        
        mock_extractor.get_dataset_info.return_value = {
            'name': 'pbp',
            'requires_year': True,
            'start_year': 1999
        }
        
        result = self.runner.invoke(main, ['extract', 'dataset', 'pbp'])
        
        assert result.exit_code == 1
        assert "requires --year parameter" in result.output
        assert "Dataset starts from year: 1999" in result.output
    
    @patch('src.cli.NFLDataExtractor')
    def test_extract_dataset_non_year_dataset(self, mock_extractor_class):
        """Test extraction of dataset that doesn't require year."""
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor
        
        mock_extractor.get_dataset_info.return_value = {
            'name': 'team_desc',
            'requires_year': False,
            'start_year': None
        }
        
        metadata = self.sample_metadata.copy()
        metadata['year'] = None
        mock_extractor.extract_dataset.return_value = (self.sample_data, metadata)
        
        result = self.runner.invoke(main, ['extract', 'dataset', 'team_desc'])
        
        assert result.exit_code == 0
        assert 'Extraction completed successfully' in result.output
        
        mock_extractor.extract_dataset.assert_called_once_with(
            dataset_name='team_desc',
            year=None,
            validate=True,
            save_to_disk=True
        )
    
    @patch('src.cli.NFLDataExtractor')
    def test_extract_dataset_with_options(self, mock_extractor_class):
        """Test extraction with various CLI options."""
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor
        
        mock_extractor.get_dataset_info.return_value = {
            'name': 'pbp',
            'requires_year': True,
            'start_year': 1999
        }
        
        mock_extractor.extract_dataset.return_value = (self.sample_data, self.sample_metadata)
        
        # Note: Click boolean flags are set to True when present, no --no- prefix needed
        result = self.runner.invoke(main, [
            'extract', 'dataset', 'pbp', '--year', '2020', '--verbose'
        ])
        
        assert result.exit_code == 0
        assert 'Preview of extracted data' in result.output  # verbose mode
        
        mock_extractor.extract_dataset.assert_called_once_with(
            dataset_name='pbp',
            year=2020,
            validate=True,  # Default value since flag is present
            save_to_disk=True  # Default value since flag is present
        )
    
    @patch('src.cli.NFLDataExtractor')
    def test_extract_dataset_extraction_error(self, mock_extractor_class):
        """Test extraction failure."""
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor
        
        mock_extractor.get_dataset_info.return_value = {
            'name': 'pbp',
            'requires_year': True,
            'start_year': 1999
        }
        
        mock_extractor.extract_dataset.side_effect = Exception("Network error")
        
        result = self.runner.invoke(main, ['extract', 'dataset', 'pbp', '--year', '2020'])
        
        assert result.exit_code == 1
        assert 'Extraction failed: Network error' in result.output
    
    @patch('src.cli.NFLDataExtractor')
    def test_extract_multiple_years_success(self, mock_extractor_class):
        """Test successful multi-year extraction."""
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor
        
        mock_extractor.get_dataset_info.return_value = {
            'name': 'pbp',
            'requires_year': True,
            'start_year': 1999
        }
        
        # Mock results for multiple years
        results = {
            2020: (self.sample_data, dict(self.sample_metadata, year=2020)),
            2021: (self.sample_data, dict(self.sample_metadata, year=2021)),
        }
        mock_extractor.extract_multiple_years.return_value = results
        
        result = self.runner.invoke(main, [
            'extract', 'multiple', 'pbp', '--years', '2020,2021'
        ])
        
        assert result.exit_code == 0
        assert 'Multi-year extraction completed' in result.output
        assert 'Summary: 2/2 years successful, 6 total rows extracted' in result.output
        
        mock_extractor.extract_multiple_years.assert_called_once_with(
            dataset_name='pbp',
            years=[2020, 2021],
            validate=True,
            save_to_disk=True
        )
    
    @patch('src.cli.NFLDataExtractor')
    def test_extract_multiple_years_invalid_format(self, mock_extractor_class):
        """Test multi-year extraction with invalid years format."""
        result = self.runner.invoke(main, [
            'extract', 'multiple', 'pbp', '--years', 'invalid,format'
        ])
        
        assert result.exit_code == 1
        assert 'Invalid years format' in result.output
        assert 'Use comma-separated format like: 2020,2021,2022' in result.output
    
    @patch('src.cli.NFLDataExtractor')
    def test_extract_multiple_years_non_year_dataset(self, mock_extractor_class):
        """Test multi-year extraction with non-year dataset."""
        mock_extractor = MagicMock()
        mock_extractor_class.return_value = mock_extractor
        
        mock_extractor.get_dataset_info.return_value = {
            'name': 'team_desc',
            'requires_year': False,
            'start_year': None
        }
        
        result = self.runner.invoke(main, [
            'extract', 'multiple', 'team_desc', '--years', '2020,2021'
        ])
        
        assert result.exit_code == 1
        assert "doesn't support year-based extraction" in result.output
    
    @patch('src.cli.ExtractionManager')
    def test_extract_incremental_success(self, mock_manager_class):
        """Test successful incremental extraction."""
        mock_manager = MagicMock()
        mock_manager_class.return_value = mock_manager
        
        mock_manager.get_recommended_years.return_value = [2024, 2023, 2022]
        mock_manager.extract_incremental.return_value = {
            'success': True,
            'years_extracted': [2022, 2023],
            'years_skipped': [2024],
            'total_rows': 1000,
            'total_files': 2,
            'errors': []
        }
        
        result = self.runner.invoke(main, ['extract', 'incremental', 'pbp'])
        
        assert result.exit_code == 0
        assert 'Completed successfully' in result.output
        assert 'Years Extracted' in result.output
        assert 'Extracted years: 2022, 2023' in result.output
        assert 'Skipped years (current): 2024' in result.output
    
    @patch('src.cli.ExtractionManager')
    def test_extract_incremental_with_years(self, mock_manager_class):
        """Test incremental extraction with specified years."""
        mock_manager = MagicMock()
        mock_manager_class.return_value = mock_manager
        
        mock_manager.extract_incremental.return_value = {
            'success': True,
            'years_extracted': [2020, 2021],
            'years_skipped': [],
            'total_rows': 2000,
            'total_files': 2,
            'errors': []
        }
        
        result = self.runner.invoke(main, [
            'extract', 'incremental', 'pbp', '--years', '2020,2021', '--force'
        ])
        
        assert result.exit_code == 0
        assert 'Force refresh enabled' in result.output
        
        mock_manager.extract_incremental.assert_called_once_with(
            dataset_name='pbp',
            years=[2020, 2021],
            force_refresh=True,
            max_age_days=1
        )
    
    @patch('src.cli.ExtractionManager')
    def test_extract_incremental_with_errors(self, mock_manager_class):
        """Test incremental extraction with some errors."""
        mock_manager = MagicMock()
        mock_manager_class.return_value = mock_manager
        
        mock_manager.get_recommended_years.return_value = [2024, 2023, 2022]
        mock_manager.extract_incremental.return_value = {
            'success': False,
            'years_extracted': [2023],
            'years_skipped': [],
            'total_rows': 500,
            'total_files': 1,
            'errors': ['Year 2024: Network timeout', 'Year 2022: API rate limit']
        }
        
        result = self.runner.invoke(main, ['extract', 'incremental', 'pbp'])
        
        assert result.exit_code == 0  # Still exits successfully even with issues
        assert 'Completed with issues' in result.output
        assert 'Errors encountered:' in result.output
        assert 'Network timeout' in result.output
        assert 'API rate limit' in result.output
    
    @patch('src.cli.ExtractionManager')
    def test_extract_status_overall(self, mock_manager_class):
        """Test overall extraction status."""
        mock_manager = MagicMock()
        mock_manager_class.return_value = mock_manager
        
        mock_manager.get_extraction_summary.return_value = {
            'total_datasets': 3,
            'total_extractions': 10,
            'successful_extractions': 8,
            'failed_extractions': 2,
            'last_extraction': '2024-01-15T10:30:00',
            'datasets': {
                'pbp': {
                    'total_years': 3,
                    'completed': 2,
                    'failed': 1,
                    'years': {
                        '2020': {'status': 'completed', 'date': '2024-01-15T10:00:00', 'rows': 1000},
                        '2021': {'status': 'completed', 'date': '2024-01-15T10:15:00', 'rows': 1200},
                        '2022': {'status': 'failed', 'date': '2024-01-15T10:30:00', 'rows': 0}
                    }
                }
            }
        }
        
        result = self.runner.invoke(main, ['extract', 'status'])
        
        assert result.exit_code == 0
        assert 'Overall extraction status' in result.output
        assert 'Total Datasets' in result.output
        assert '3' in result.output
        assert 'Total Extractions' in result.output
        assert '10' in result.output
    
    @patch('src.cli.ExtractionManager')
    def test_extract_status_specific_dataset(self, mock_manager_class):
        """Test status for specific dataset."""
        mock_manager = MagicMock()
        mock_manager_class.return_value = mock_manager
        
        mock_manager.get_extraction_summary.return_value = {
            'datasets': {
                'pbp': {
                    'total_years': 2,
                    'completed': 1,
                    'failed': 1,
                    'years': {
                        '2020': {'status': 'completed', 'date': '2024-01-15T10:00:00', 'rows': 1000},
                        '2021': {'status': 'failed', 'date': '2024-01-15T10:15:00', 'rows': 0}
                    }
                }
            }
        }
        
        result = self.runner.invoke(main, ['extract', 'status', 'pbp'])
        
        assert result.exit_code == 0
        assert 'Extraction status for: pbp' in result.output
        assert 'Dataset: pbp' in result.output
        assert '✓ completed' in result.output
        assert '✗ failed' in result.output
    
    @patch('src.cli.ExtractionManager')
    def test_extract_status_no_data(self, mock_manager_class):
        """Test status when no extractions recorded."""
        mock_manager = MagicMock()
        mock_manager_class.return_value = mock_manager
        
        mock_manager.get_extraction_summary.return_value = {
            'total_extractions': 0,
            'datasets': {}
        }
        
        result = self.runner.invoke(main, ['extract', 'status'])
        
        assert result.exit_code == 0
        assert 'No extractions recorded' in result.output
    
    @patch('src.cli.ExtractionManager')
    def test_extract_cleanup_success(self, mock_manager_class):
        """Test successful cleanup operation."""
        mock_manager = MagicMock()
        mock_manager_class.return_value = mock_manager
        
        mock_manager.cleanup_old_extractions.return_value = {
            'files_deleted': 5,
            'bytes_freed': 10485760,  # 10 MB
            'state_entries_cleaned': 3,
            'errors': []
        }
        
        result = self.runner.invoke(main, ['extract', 'cleanup', '--max-age-days', '7'])
        
        assert result.exit_code == 0
        assert 'Cleanup completed' in result.output
        assert 'Files Deleted' in result.output
        assert '5' in result.output
        assert 'Bytes Freed' in result.output
        assert '10,485,760' in result.output
        
        mock_manager.cleanup_old_extractions.assert_called_once_with(max_age_days=7)
    
    @patch('src.cli.ExtractionManager')
    def test_extract_cleanup_dry_run(self, mock_manager_class):
        """Test cleanup dry run."""
        mock_manager = MagicMock()
        mock_manager_class.return_value = mock_manager
        
        result = self.runner.invoke(main, ['extract', 'cleanup', '--dry-run'])
        
        assert result.exit_code == 0
        assert 'DRY RUN - No files will be deleted' in result.output
        assert 'Dry run functionality would be implemented here' in result.output
        
        # Should not call actual cleanup
        mock_manager.cleanup_old_extractions.assert_not_called()
    
    @patch('src.cli.ExtractionManager')
    def test_extract_cleanup_with_errors(self, mock_manager_class):
        """Test cleanup with some errors."""
        mock_manager = MagicMock()
        mock_manager_class.return_value = mock_manager
        
        mock_manager.cleanup_old_extractions.return_value = {
            'files_deleted': 2,
            'bytes_freed': 1048576,
            'state_entries_cleaned': 2,
            'errors': ['Permission denied for file1', 'File not found: file2']
        }
        
        result = self.runner.invoke(main, ['extract', 'cleanup', '--verbose'])
        
        assert result.exit_code == 0
        assert 'Cleanup completed' in result.output
        assert 'Permission denied for file1' in result.output
        assert 'File not found: file2' in result.output