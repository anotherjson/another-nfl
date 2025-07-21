"""Tests for ParquetReader module."""
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import tempfile
import os

from src.parquet_reader import ParquetReader


class TestParquetReader:
    """Test cases for ParquetReader class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.reader = ParquetReader()
        
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        
        # Create sample data
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 28, 32],
            'score': [85.5, 92.0, 78.5, 88.0, 95.5]
        })
        
        # Create test parquet file
        self.test_file = self.temp_path / "test_data.parquet"
        self.sample_data.to_parquet(self.test_file, index=False)
        
        # Create empty parquet file
        self.empty_file = self.temp_path / "empty_data.parquet"
        empty_df = pd.DataFrame()
        empty_df.to_parquet(self.empty_file, index=False)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        # Remove temporary files
        for file in self.temp_path.glob("*.parquet"):
            file.unlink()
        os.rmdir(self.temp_dir)
    
    def test_read_parquet_success(self):
        """Test successful parquet file reading."""
        result = self.reader.read_parquet(self.test_file, limit=3)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == ['id', 'name', 'age', 'score']
        assert result['id'].tolist() == [1, 2, 3]
    
    def test_read_parquet_no_limit(self):
        """Test reading parquet file without limit."""
        result = self.reader.read_parquet(self.test_file, limit=0)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5  # All rows
        assert list(result.columns) == ['id', 'name', 'age', 'score']
    
    def test_read_parquet_limit_larger_than_data(self):
        """Test reading with limit larger than available data."""
        result = self.reader.read_parquet(self.test_file, limit=10)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5  # All available rows
    
    def test_read_parquet_empty_file(self):
        """Test reading empty parquet file."""
        result = self.reader.read_parquet(self.empty_file)
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_read_parquet_file_not_found(self):
        """Test reading non-existent file."""
        non_existent = self.temp_path / "non_existent.parquet"
        
        with pytest.raises(FileNotFoundError, match="File not found"):
            self.reader.read_parquet(non_existent)
    
    def test_read_parquet_invalid_file(self):
        """Test reading invalid parquet file."""
        # Create a text file with .parquet extension
        invalid_file = self.temp_path / "invalid.parquet"
        with open(invalid_file, 'w') as f:
            f.write("This is not a parquet file")
        
        with pytest.raises(Exception, match="Failed to read parquet file"):
            self.reader.read_parquet(invalid_file)
    
    def test_get_file_info_success(self):
        """Test getting file information successfully."""
        info = self.reader.get_file_info(self.test_file)
        
        assert isinstance(info, dict)
        assert "File Name" in info
        assert "File Size (MB)" in info
        assert "Modified" in info
        assert "Total Rows" in info
        assert "Total Columns" in info
        assert "Row Groups" in info
        assert "Columns" in info
        assert "Column Types" in info
        
        assert info["File Name"] == "test_data.parquet"
        assert info["Total Rows"] == 5
        assert info["Total Columns"] == 4
        assert info["Columns"] == ['id', 'name', 'age', 'score']
        assert isinstance(info["File Size (MB)"], float)
        assert isinstance(info["Row Groups"], int)
    
    def test_get_file_info_file_not_found(self):
        """Test getting info for non-existent file."""
        non_existent = self.temp_path / "non_existent.parquet"
        
        with pytest.raises(FileNotFoundError, match="File not found"):
            self.reader.get_file_info(non_existent)
    
    def test_get_file_info_invalid_file(self):
        """Test getting info for invalid parquet file."""
        invalid_file = self.temp_path / "invalid.parquet"
        with open(invalid_file, 'w') as f:
            f.write("This is not a parquet file")
        
        with pytest.raises(Exception, match="Failed to get file info"):
            self.reader.get_file_info(invalid_file)
    
    def test_get_column_stats_success(self):
        """Test getting column statistics successfully."""
        stats = self.reader.get_column_stats(self.test_file)
        
        assert isinstance(stats, dict)
        assert "shape" in stats
        assert "memory_usage_mb" in stats
        assert "dtypes" in stats
        assert "null_counts" in stats
        assert "null_percentages" in stats
        assert "numeric_stats" in stats
        
        assert stats["shape"] == (5, 4)
        assert isinstance(stats["memory_usage_mb"], float)
        assert stats["dtypes"]["id"] == 'int64'
        assert stats["dtypes"]["name"] == 'object'
        assert stats["null_counts"]["id"] == 0
        assert stats["null_percentages"]["id"] == 0.0
    
    def test_get_column_stats_with_nulls(self):
        """Test column statistics with null values."""
        # Create data with nulls
        data_with_nulls = pd.DataFrame({
            'id': [1, 2, None, 4, 5],
            'name': ['Alice', None, 'Charlie', 'David', None],
            'score': [85.5, None, 78.5, 88.0, 95.5]
        })
        
        null_file = self.temp_path / "data_with_nulls.parquet"
        data_with_nulls.to_parquet(null_file, index=False)
        
        stats = self.reader.get_column_stats(null_file)
        
        assert stats["null_counts"]["id"] == 1
        assert stats["null_counts"]["name"] == 2
        assert stats["null_counts"]["score"] == 1
        assert stats["null_percentages"]["name"] == 40.0  # 2/5 * 100
    
    def test_get_column_stats_no_numeric_columns(self):
        """Test column statistics with no numeric columns."""
        text_data = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie'],
            'city': ['New York', 'London', 'Tokyo'],
            'country': ['USA', 'UK', 'Japan']
        })
        
        text_file = self.temp_path / "text_data.parquet"
        text_data.to_parquet(text_file, index=False)
        
        stats = self.reader.get_column_stats(text_file)
        
        assert "numeric_stats" not in stats or stats.get("numeric_stats") == {}
    
    def test_get_column_stats_file_not_found(self):
        """Test column statistics for non-existent file."""
        non_existent = self.temp_path / "non_existent.parquet"
        
        with pytest.raises(Exception, match="Failed to get column stats"):
            self.reader.get_column_stats(non_existent)
    
    def test_column_types_in_file_info(self):
        """Test that column types are correctly identified."""
        info = self.reader.get_file_info(self.test_file)
        
        column_types = info["Column Types"]
        assert 'id' in column_types
        assert 'name' in column_types
        assert 'age' in column_types
        assert 'score' in column_types
        
        # Verify types are strings
        for col_type in column_types.values():
            assert isinstance(col_type, str)