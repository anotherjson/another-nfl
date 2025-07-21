"""NFL Data Explorer module for interfacing with nfl_data_py."""
from typing import Dict, List, Optional, Any

import pandas as pd
import nfl_data_py as nfl


class NFLExplorer:
    """Handles exploration and data fetching from NFL datasets."""
    
    def __init__(self):
        """Initialize the NFL Explorer."""
        self._datasets = self._initialize_datasets()
    
    def _initialize_datasets(self) -> List[Dict[str, Any]]:
        """Initialize list of available NFL datasets with metadata."""
        return [
            {
                "name": "pbp",
                "description": "Play-by-play data",
                "start_year": 1999,
                "function": nfl.import_pbp_data
            },
            {
                "name": "weekly",
                "description": "Weekly player stats",
                "start_year": 1999,
                "function": nfl.import_weekly_data
            },
            {
                "name": "seasonal",
                "description": "Seasonal player stats",
                "start_year": 1999,
                "function": nfl.import_seasonal_data
            },
            {
                "name": "weekly_rosters",
                "description": "Weekly team rosters",
                "start_year": 1999,
                "function": nfl.import_weekly_rosters
            },
            {
                "name": "seasonal_rosters",
                "description": "Seasonal team rosters",
                "start_year": 1999,
                "function": nfl.import_seasonal_rosters
            },
            {
                "name": "schedules",
                "description": "Game schedules",
                "start_year": 1999,
                "function": nfl.import_schedules
            },
            {
                "name": "team_desc",
                "description": "Team descriptions and info",
                "start_year": None,
                "function": nfl.import_team_desc
            },
            {
                "name": "officials",
                "description": "Game officials",
                "start_year": 2001,
                "function": nfl.import_officials
            },
            {
                "name": "combine",
                "description": "NFL Combine results",
                "start_year": 1987,
                "function": nfl.import_combine_data
            },
            {
                "name": "draft_picks",
                "description": "NFL Draft picks",
                "start_year": 1936,
                "function": nfl.import_draft_picks
            },
            {
                "name": "qbr",
                "description": "Weekly QBR data",
                "start_year": 2006,
                "function": nfl.import_qbr
            },
            {
                "name": "weekly_pfr",
                "description": "Pro Football Reference weekly stats",
                "start_year": 1932,
                "function": nfl.import_weekly_pfr
            },
            {
                "name": "seasonal_pfr",
                "description": "Pro Football Reference seasonal stats",
                "start_year": 1932,
                "function": nfl.import_seasonal_pfr
            },
            {
                "name": "injuries",
                "description": "Player injury reports",
                "start_year": 2009,
                "function": nfl.import_injuries
            },
            {
                "name": "depth_charts",
                "description": "Team depth charts",
                "start_year": 2001,
                "function": nfl.import_depth_charts
            },
            {
                "name": "snap_counts",
                "description": "Player snap counts",
                "start_year": 2012,
                "function": nfl.import_snap_counts
            },
            {
                "name": "ftn_data",
                "description": "Fantasy Points allowed data",
                "start_year": 2018,
                "function": nfl.import_ftn_data
            },
            {
                "name": "ngs_data",
                "description": "Next Gen Stats data",
                "start_year": 2016,
                "function": nfl.import_ngs_data
            },
            {
                "name": "players",
                "description": "Player information",
                "start_year": None,
                "function": nfl.import_players
            }
        ]
    
    def list_datasets(self) -> List[Dict[str, Any]]:
        """Return list of all available datasets."""
        return [
            {
                "name": dataset["name"],
                "description": dataset["description"],
                "start_year": dataset["start_year"]
            }
            for dataset in self._datasets
        ]
    
    def is_valid_dataset(self, dataset_name: str) -> bool:
        """Check if dataset name is valid."""
        return any(d["name"] == dataset_name for d in self._datasets)
    
    def get_dataset_info(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific dataset."""
        for dataset in self._datasets:
            if dataset["name"] == dataset_name:
                return {
                    "name": dataset["name"],
                    "description": dataset["description"],
                    "start_year": dataset["start_year"]
                }
        return None
    
    def get_sample_data(
        self, 
        dataset_name: str, 
        year: Optional[int] = None,
        limit: int = 5
    ) -> pd.DataFrame:
        """
        Fetch sample data from specified dataset.
        
        Args:
            dataset_name: Name of the dataset to fetch
            year: Optional year to fetch data for
            limit: Number of rows to return
            
        Returns:
            DataFrame containing sample data
            
        Raises:
            ValueError: If dataset is invalid or year is out of range
            Exception: If data fetching fails
        """
        if not self.is_valid_dataset(dataset_name):
            raise ValueError(f"Invalid dataset: {dataset_name}")
        
        dataset_info = next(d for d in self._datasets if d["name"] == dataset_name)
        
        # Validate year if provided
        if year is not None and dataset_info["start_year"] is not None:
            if year < dataset_info["start_year"]:
                raise ValueError(
                    f"Year {year} is before dataset start year "
                    f"{dataset_info['start_year']}"
                )
        
        try:
            # Get the function for this dataset
            fetch_function = dataset_info["function"]
            
            # Determine parameters based on dataset and year
            if dataset_name in ["team_desc"]:
                # Some datasets don't take year parameter
                data = fetch_function()
            elif year is not None:
                data = fetch_function([year])
            else:
                # Get recent year for sample data
                import datetime
                current_year = datetime.datetime.now().year
                if dataset_info["start_year"]:
                    sample_year = min(current_year - 1, current_year)
                    data = fetch_function([sample_year])
                else:
                    data = fetch_function()
            
            if isinstance(data, pd.DataFrame) and not data.empty:
                return data.head(limit)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            raise Exception(f"Failed to fetch data from {dataset_name}: {str(e)}")