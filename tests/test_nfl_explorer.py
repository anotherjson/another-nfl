"""Tests for NFLExplorer module."""
import pytest
import pandas as pd
from unittest.mock import patch, Mock

from src.nfl_explorer import NFLExplorer


class TestNFLExplorer:
    """Test cases for NFLExplorer class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.explorer = NFLExplorer()
    
    def test_initialization(self):
        """Test that NFLExplorer initializes correctly."""
        assert isinstance(self.explorer._datasets, list)
        assert len(self.explorer._datasets) > 0
        
        # Check that all datasets have required fields
        for dataset in self.explorer._datasets:
            assert "name" in dataset
            assert "description" in dataset
            assert "start_year" in dataset
            assert "function" in dataset
    
    def test_list_datasets(self):
        """Test that list_datasets returns expected structure."""
        datasets = self.explorer.list_datasets()
        
        assert isinstance(datasets, list)
        assert len(datasets) > 0
        
        for dataset in datasets:
            assert "name" in dataset
            assert "description" in dataset
            assert "start_year" in dataset
            assert isinstance(dataset["name"], str)
            assert isinstance(dataset["description"], str)
    
    def test_is_valid_dataset(self):
        """Test dataset validation."""
        # Test valid datasets
        assert self.explorer.is_valid_dataset("pbp") is True
        assert self.explorer.is_valid_dataset("weekly") is True
        assert self.explorer.is_valid_dataset("seasonal") is True
        
        # Test invalid datasets
        assert self.explorer.is_valid_dataset("invalid_dataset") is False
        assert self.explorer.is_valid_dataset("") is False
        assert self.explorer.is_valid_dataset("nonexistent") is False
    
    def test_get_dataset_info(self):
        """Test getting dataset information."""
        # Test valid dataset
        info = self.explorer.get_dataset_info("pbp")
        assert info is not None
        assert info["name"] == "pbp"
        assert "description" in info
        assert "start_year" in info
        
        # Test invalid dataset
        info = self.explorer.get_dataset_info("invalid_dataset")
        assert info is None
    
    def test_get_sample_data_invalid_dataset(self):
        """Test that invalid dataset raises ValueError."""
        with pytest.raises(ValueError, match="Invalid dataset"):
            self.explorer.get_sample_data("invalid_dataset")
    
    def test_get_sample_data_invalid_year(self):
        """Test that invalid year raises ValueError."""
        with pytest.raises(ValueError, match="Year .* is before dataset start year"):
            self.explorer.get_sample_data("pbp", year=1990)  # Before 1999 start year
    
    @patch.object(NFLExplorer, '_initialize_datasets')
    def test_get_sample_data_with_year(self, mock_init):
        """Test getting sample data with specific year."""
        # Mock the return data and function
        mock_df = pd.DataFrame({
            'game_id': ['2020_01_TB_NO'],
            'play_id': [1],
            'down': [1]
        })
        mock_function = Mock(return_value=mock_df)
        
        # Set up mock dataset
        mock_init.return_value = [{
            "name": "pbp",
            "description": "Play-by-play data",
            "start_year": 1999,
            "function": mock_function,
        }]
        
        # Create new explorer with mocked datasets
        explorer = NFLExplorer()
        result = explorer.get_sample_data("pbp", year=2020, limit=3)
        
        mock_function.assert_called_once_with([2020])
        assert isinstance(result, pd.DataFrame)
        assert len(result) <= 3
    
    @patch.object(NFLExplorer, '_initialize_datasets')
    def test_get_sample_data_no_year_parameter(self, mock_init):
        """Test getting sample data for dataset that doesn't take year parameter."""
        # Mock the return data and function
        mock_df = pd.DataFrame({
            'team_abbr': ['TB', 'NO'],
            'team_name': ['Tampa Bay Buccaneers', 'New Orleans Saints']
        })
        mock_function = Mock(return_value=mock_df)
        
        # Set up mock dataset
        mock_init.return_value = [{
            "name": "team_desc",
            "description": "Team descriptions",
            "start_year": None,
            "function": mock_function,
        }]
        
        # Create new explorer with mocked datasets
        explorer = NFLExplorer()
        result = explorer.get_sample_data("team_desc", limit=2)
        
        mock_function.assert_called_once_with()
        assert isinstance(result, pd.DataFrame)
        assert len(result) <= 2
    
    @patch.object(NFLExplorer, '_initialize_datasets')
    def test_get_sample_data_no_year_provided(self, mock_init):
        """Test getting sample data without providing year."""
        # Mock the return data and function
        mock_df = pd.DataFrame({
            'player_id': ['123', '456'],
            'player_name': ['Tom Brady', 'Drew Brees'],
            'week': [1, 1]
        })
        mock_function = Mock(return_value=mock_df)
        
        # Set up mock dataset
        mock_init.return_value = [{
            "name": "weekly",
            "description": "Weekly player stats",
            "start_year": 1999,
            "function": mock_function,
        }]
        
        # Create new explorer with mocked datasets
        explorer = NFLExplorer()
        result = explorer.get_sample_data("weekly", limit=2)
        
        # Should be called with recent year
        mock_function.assert_called_once()
        call_args = mock_function.call_args[0][0]
        assert isinstance(call_args, list)
        assert len(call_args) == 1
        assert isinstance(call_args[0], int)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) <= 2
    
    @patch.object(NFLExplorer, '_initialize_datasets')
    def test_get_sample_data_empty_result(self, mock_init):
        """Test handling of empty data result."""
        # Mock empty DataFrame and function
        mock_function = Mock(return_value=pd.DataFrame())
        
        # Set up mock dataset
        mock_init.return_value = [{
            "name": "pbp",
            "description": "Play-by-play data",
            "start_year": 1999,
            "function": mock_function,
        }]
        
        # Create new explorer with mocked datasets
        explorer = NFLExplorer()
        result = explorer.get_sample_data("pbp", year=2020)
        
        mock_function.assert_called_once_with([2020])
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    @patch.object(NFLExplorer, '_initialize_datasets')
    def test_get_sample_data_exception_handling(self, mock_init):
        """Test exception handling in get_sample_data."""
        # Mock function to raise exception
        mock_function = Mock(side_effect=Exception("Network error"))
        
        # Set up mock dataset
        mock_init.return_value = [{
            "name": "pbp",
            "description": "Play-by-play data",
            "start_year": 1999,
            "function": mock_function,
        }]
        
        # Create new explorer with mocked datasets
        explorer = NFLExplorer()
        
        with pytest.raises(Exception, match="Failed to fetch data from pbp"):
            explorer.get_sample_data("pbp", year=2020)
    
    def test_dataset_names_are_unique(self):
        """Test that all dataset names are unique."""
        dataset_names = [d["name"] for d in self.explorer._datasets]
        assert len(dataset_names) == len(set(dataset_names))
    
    def test_all_datasets_have_functions(self):
        """Test that all datasets have valid function references."""
        for dataset in self.explorer._datasets:
            assert callable(dataset["function"])
    
    def test_start_years_are_valid(self):
        """Test that start years are reasonable or None."""
        for dataset in self.explorer._datasets:
            start_year = dataset["start_year"]
            if start_year is not None:
                assert isinstance(start_year, int)
                assert 1900 <= start_year <= 2030  # Reasonable range