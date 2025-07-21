"""Tests for CLI module."""
import pytest
import pandas as pd
from click.testing import CliRunner
from unittest.mock import patch, Mock
import tempfile
import os
from pathlib import Path

from src.cli import main


class TestCLI:
    """Test cases for CLI commands."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
        
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        
        # Create test parquet file
        test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'score': [85.5, 92.0, 78.5]
        })
        self.test_parquet = self.temp_path / "test.parquet"
        test_data.to_parquet(self.test_parquet, index=False)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        # Remove temporary files
        for file in self.temp_path.glob("*.parquet"):
            file.unlink()
        os.rmdir(self.temp_dir)
    
    def test_main_command_help(self):
        """Test main command help."""
        result = self.runner.invoke(main, ['--help'])
        assert result.exit_code == 0
        assert "NFL Data CLI tool" in result.output
    
    def test_explore_command_help(self):
        """Test explore subcommand help."""
        result = self.runner.invoke(main, ['explore', '--help'])
        assert result.exit_code == 0
        assert "Explore NFL datasets" in result.output
    
    @patch('src.cli.NFLExplorer')
    def test_explore_datasets_success(self, mock_explorer_class):
        """Test successful datasets listing."""
        # Mock the explorer instance
        mock_explorer = Mock()
        mock_explorer_class.return_value = mock_explorer
        mock_explorer.list_datasets.return_value = [
            {"name": "pbp", "description": "Play-by-play data", "start_year": 1999},
            {"name": "weekly", "description": "Weekly stats", "start_year": 1999}
        ]
        
        result = self.runner.invoke(main, ['explore', 'datasets'])
        
        assert result.exit_code == 0
        assert "Available NFL Datasets" in result.output
        assert "pbp" in result.output
        assert "weekly" in result.output
    
    @patch('src.cli.NFLExplorer')
    def test_explore_datasets_error(self, mock_explorer_class):
        """Test datasets listing with error."""
        mock_explorer_class.side_effect = Exception("Connection error")
        
        result = self.runner.invoke(main, ['explore', 'datasets'])
        
        assert result.exit_code == 1
        assert "Error listing datasets" in result.output
    
    @patch('src.cli.NFLExplorer')
    def test_explore_data_success(self, mock_explorer_class):
        """Test successful data exploration."""
        mock_explorer = Mock()
        mock_explorer_class.return_value = mock_explorer
        mock_explorer.is_valid_dataset.return_value = True
        
        sample_data = pd.DataFrame({
            'game_id': ['2020_01_TB_NO'],
            'play_id': [1],
            'down': [1]
        })
        mock_explorer.get_sample_data.return_value = sample_data
        
        result = self.runner.invoke(main, ['explore', 'data', 'pbp', '--year', '2020'])
        
        assert result.exit_code == 0
        assert "Fetching data for dataset: pbp" in result.output
        assert "Year: 2020" in result.output
        assert "Sample data" in result.output
    
    @patch('src.cli.NFLExplorer')
    def test_explore_data_invalid_dataset(self, mock_explorer_class):
        """Test data exploration with invalid dataset."""
        mock_explorer = Mock()
        mock_explorer_class.return_value = mock_explorer
        mock_explorer.is_valid_dataset.return_value = False
        mock_explorer.list_datasets.return_value = [
            {"name": "pbp", "description": "Play-by-play data", "start_year": 1999}
        ]
        
        result = self.runner.invoke(main, ['explore', 'data', 'invalid_dataset'])
        
        assert result.exit_code == 1
        assert "Invalid dataset: invalid_dataset" in result.output
        assert "Available datasets:" in result.output
    
    @patch('src.cli.NFLExplorer')
    def test_explore_data_empty_result(self, mock_explorer_class):
        """Test data exploration with empty result."""
        mock_explorer = Mock()
        mock_explorer_class.return_value = mock_explorer
        mock_explorer.is_valid_dataset.return_value = True
        mock_explorer.get_sample_data.return_value = pd.DataFrame()
        
        result = self.runner.invoke(main, ['explore', 'data', 'pbp'])
        
        assert result.exit_code == 0
        assert "No data found for pbp" in result.output
    
    @patch('src.cli.NFLExplorer')
    def test_explore_data_error(self, mock_explorer_class):
        """Test data exploration with error."""
        mock_explorer = Mock()
        mock_explorer_class.return_value = mock_explorer
        mock_explorer.is_valid_dataset.return_value = True
        mock_explorer.get_sample_data.side_effect = Exception("Network error")
        
        result = self.runner.invoke(main, ['explore', 'data', 'pbp'])
        
        assert result.exit_code == 1
        assert "Error fetching data" in result.output
    
    @patch('src.cli.NFLExplorer')
    def test_explore_data_error_verbose(self, mock_explorer_class):
        """Test data exploration with error in verbose mode."""
        mock_explorer = Mock()
        mock_explorer_class.return_value = mock_explorer
        mock_explorer.is_valid_dataset.return_value = True
        mock_explorer.get_sample_data.side_effect = ValueError("Invalid year")
        
        result = self.runner.invoke(main, ['explore', 'data', 'pbp', '--verbose'])
        
        assert result.exit_code == 1
        assert "Detailed error: ValueError: Invalid year" in result.output
        assert "Traceback:" in result.output
    
    @patch('src.cli.ParquetReader')
    def test_read_parquet_success(self, mock_reader_class):
        """Test successful parquet file reading."""
        mock_reader = Mock()
        mock_reader_class.return_value = mock_reader
        
        sample_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        mock_reader.read_parquet.return_value = sample_data
        
        result = self.runner.invoke(main, ['read', str(self.test_parquet)])
        
        assert result.exit_code == 0
        assert f"Reading parquet file: {self.test_parquet}" in result.output
        assert "Data preview" in result.output
    
    @patch('src.cli.ParquetReader')
    def test_read_parquet_with_info(self, mock_reader_class):
        """Test parquet file reading with info flag."""
        mock_reader = Mock()
        mock_reader_class.return_value = mock_reader
        
        sample_data = pd.DataFrame({'id': [1, 2, 3]})
        mock_reader.read_parquet.return_value = sample_data
        mock_reader.get_file_info.return_value = {
            "File Name": "test.parquet",
            "Total Rows": 3,
            "Total Columns": 1
        }
        
        result = self.runner.invoke(main, ['read', str(self.test_parquet), '--info'])
        
        assert result.exit_code == 0
        assert "Parquet File Information" in result.output
        assert "File Name" in result.output
    
    @patch('src.cli.ParquetReader')
    def test_read_parquet_empty_file(self, mock_reader_class):
        """Test reading empty parquet file."""
        mock_reader = Mock()
        mock_reader_class.return_value = mock_reader
        mock_reader.read_parquet.return_value = pd.DataFrame()
        
        result = self.runner.invoke(main, ['read', str(self.test_parquet)])
        
        assert result.exit_code == 0
        assert "File is empty or contains no data" in result.output
    
    def test_read_parquet_file_not_found(self):
        """Test reading non-existent parquet file."""
        non_existent = self.temp_path / "non_existent.parquet"
        
        result = self.runner.invoke(main, ['read', str(non_existent)])
        
        assert result.exit_code == 2  # Click's file not found exit code
    
    @patch('src.cli.ParquetReader')
    def test_read_parquet_error(self, mock_reader_class):
        """Test parquet reading with error."""
        mock_reader = Mock()
        mock_reader_class.return_value = mock_reader
        mock_reader.read_parquet.side_effect = Exception("Read error")
        
        result = self.runner.invoke(main, ['read', str(self.test_parquet)])
        
        assert result.exit_code == 1
        assert "Error reading parquet file" in result.output
    
    def test_explore_data_with_limit(self):
        """Test data exploration with custom limit."""
        with patch('src.cli.NFLExplorer') as mock_explorer_class:
            mock_explorer = Mock()
            mock_explorer_class.return_value = mock_explorer
            mock_explorer.is_valid_dataset.return_value = True
            mock_explorer.get_sample_data.return_value = pd.DataFrame({'id': [1]})
            
            result = self.runner.invoke(main, ['explore', 'data', 'pbp', '--limit', '10'])
            
            assert result.exit_code == 0
            mock_explorer.get_sample_data.assert_called_with('pbp', year=None, limit=10)
    
    def test_read_parquet_with_limit(self):
        """Test parquet reading with custom limit."""
        with patch('src.cli.ParquetReader') as mock_reader_class:
            mock_reader = Mock()
            mock_reader_class.return_value = mock_reader
            mock_reader.read_parquet.return_value = pd.DataFrame({'id': [1]})
            
            result = self.runner.invoke(main, ['read', str(self.test_parquet), '--limit', '5'])
            
            assert result.exit_code == 0
            mock_reader.read_parquet.assert_called_with(self.test_parquet, limit=5)