"""
NFL Data Extractor for production data extraction with configuration-driven approach.
"""

import pandas as pd
import nfl_data_py as nfl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import time
import logging

from .config_loader import ConfigLoader, DatasetConfig


class NFLDataExtractor:
    """Production data extractor for NFL datasets with configuration-driven approach."""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialize the NFL Data Extractor.
        
        Args:
            config_dir: Path to configuration directory. Defaults to configs/datasets
        """
        self.config_loader = ConfigLoader(config_dir)
        self._setup_logging()
        self._function_map = self._create_function_map()
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def _create_function_map(self) -> Dict[str, Any]:
        """Create mapping of function names to actual nfl_data_py functions."""
        return {
            'import_pbp_data': nfl.import_pbp_data,
            'import_weekly_data': nfl.import_weekly_data,
            'import_seasonal_data': nfl.import_seasonal_data,
            'import_weekly_rosters': nfl.import_weekly_rosters,
            'import_seasonal_rosters': nfl.import_seasonal_rosters,
            'import_schedules': nfl.import_schedules,
            'import_team_desc': nfl.import_team_desc,
            'import_officials': nfl.import_officials,
            'import_combine_data': nfl.import_combine_data,
            'import_draft_picks': nfl.import_draft_picks,
            'import_qbr': nfl.import_qbr,
            'import_pfr_weekly': nfl.import_weekly_pfr,
            'import_pfr_seasonal': nfl.import_seasonal_pfr,
            'import_injuries': nfl.import_injuries,
            'import_depth_charts': nfl.import_depth_charts,
            'import_snap_counts': nfl.import_snap_counts,
            'import_ftn_data': nfl.import_ftn_data,
            'import_ngs_data': nfl.import_ngs_data,
            'import_players': nfl.import_players,
        }
    
    def extract_dataset(
        self,
        dataset_name: str,
        year: Optional[int] = None,
        etl_date: Optional[str] = None,
        validate: bool = True,
        save_to_disk: bool = True
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Extract data for a specific dataset with full production capabilities.
        
        Args:
            dataset_name: Name of the dataset to extract
            year: Year to extract (if required by dataset)
            etl_date: ETL date for partitioning (defaults to current date)
            validate: Whether to validate the extracted data
            save_to_disk: Whether to save extracted data to disk
            
        Returns:
            Tuple of (extracted_data, extraction_metadata)
            
        Raises:
            ValueError: If dataset or year is invalid
            Exception: If extraction fails
        """
        start_time = time.time()
        
        # Get dataset configuration
        config = self.config_loader.get_dataset_config(dataset_name)
        
        # Validate year if required
        if config.requires_year and year is not None:
            self.config_loader.validate_year_for_dataset(dataset_name, year)
        elif config.requires_year and year is None:
            raise ValueError(f"Dataset '{dataset_name}' requires a year parameter")
        
        # Generate ETL date if not provided
        if etl_date is None:
            etl_date = datetime.now().strftime('%Y-%m-%d')
        
        self.logger.info(f"Starting extraction for {dataset_name}, year={year}, etl_date={etl_date}")
        
        try:
            # Extract data with retry logic
            data = self._extract_with_retry(config, year)
            
            # Validate data if requested
            extraction_metadata = {
                'dataset': dataset_name,
                'year': year,
                'etl_date': etl_date,
                'extraction_time': datetime.now().isoformat(),
                'duration_seconds': round(time.time() - start_time, 2),
                'rows_extracted': len(data) if not data.empty else 0,
                'columns': list(data.columns) if not data.empty else [],
                'file_size_mb': 0,  # Will be updated if saved to disk
                'validation_passed': False,  # Will be updated after validation
                'output_path': None  # Will be updated if saved to disk
            }
            
            if validate and not data.empty:
                validation_result = self._validate_data(data, config)
                extraction_metadata['validation_passed'] = validation_result['passed']
                extraction_metadata['validation_details'] = validation_result
                
                if not validation_result['passed']:
                    self.logger.warning(f"Data validation failed for {dataset_name}: {validation_result}")
            else:
                extraction_metadata['validation_passed'] = True  # Skip validation or empty data
            
            # Save to disk if requested
            if save_to_disk and not data.empty:
                output_path = self._save_to_parquet(data, config, year, etl_date)
                extraction_metadata['output_path'] = str(output_path)
                
                # Calculate file size
                if output_path.exists():
                    file_size_bytes = output_path.stat().st_size
                    extraction_metadata['file_size_mb'] = round(file_size_bytes / (1024 * 1024), 2)
            
            self.logger.info(
                f"Extraction completed for {dataset_name}: "
                f"{extraction_metadata['rows_extracted']} rows in "
                f"{extraction_metadata['duration_seconds']}s"
            )
            
            return data, extraction_metadata
            
        except Exception as e:
            self.logger.error(f"Extraction failed for {dataset_name}: {str(e)}")
            raise Exception(f"Failed to extract {dataset_name}: {str(e)}")
    
    def _extract_with_retry(self, config: DatasetConfig, year: Optional[int] = None) -> pd.DataFrame:
        """
        Extract data with retry logic based on configuration.
        
        Args:
            config: Dataset configuration
            year: Year to extract (if applicable)
            
        Returns:
            Extracted DataFrame
            
        Raises:
            Exception: If all retry attempts fail
        """
        function = self._function_map.get(config.function_name)
        if not function:
            raise ValueError(f"Unknown function: {config.function_name}")
        
        retry_attempts = config.extraction.get('retry_attempts', 3)
        timeout_seconds = config.extraction.get('timeout_seconds', 120)
        
        for attempt in range(retry_attempts):
            try:
                self.logger.info(f"Extraction attempt {attempt + 1}/{retry_attempts} for {config.name}")
                
                # Set timeout (implementation would depend on specific requirements)
                if config.requires_year and year is not None:
                    data = function([year])
                else:
                    data = function()
                
                # Ensure we have a DataFrame
                if not isinstance(data, pd.DataFrame):
                    raise ValueError(f"Expected DataFrame, got {type(data)}")
                
                return data
                
            except Exception as e:
                self.logger.warning(
                    f"Attempt {attempt + 1} failed for {config.name}: {str(e)}"
                )
                if attempt == retry_attempts - 1:  # Last attempt
                    raise e
                
                # Wait before retry (exponential backoff)
                wait_time = 2 ** attempt
                self.logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
        
        raise Exception(f"All {retry_attempts} attempts failed for {config.name}")
    
    def _validate_data(self, data: pd.DataFrame, config: DatasetConfig) -> Dict[str, Any]:
        """
        Validate extracted data based on configuration requirements.
        
        Args:
            data: DataFrame to validate
            config: Dataset configuration with validation rules
            
        Returns:
            Dictionary containing validation results
        """
        validation_result = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'row_count': len(data),
            'column_count': len(data.columns),
            'missing_columns': [],
            'unexpected_columns': [],
            'size_check': True
        }
        
        # Check required columns
        required_columns = config.validation.get('required_columns', [])
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            validation_result['missing_columns'] = missing_columns
            validation_result['errors'].append(f"Missing required columns: {missing_columns}")
            validation_result['passed'] = False
        
        # Check for empty data
        if data.empty:
            validation_result['errors'].append("Dataset is empty")
            validation_result['passed'] = False
        
        # Check estimated size (soft warning)
        expected_size_mb = config.validation.get('expected_size_mb', 0)
        if expected_size_mb > 0:
            # Rough estimate: 8 bytes per value * rows * columns
            estimated_size_mb = (len(data) * len(data.columns) * 8) / (1024 * 1024)
            size_ratio = estimated_size_mb / expected_size_mb
            
            if size_ratio < 0.5 or size_ratio > 2.0:  # More than 50% difference
                validation_result['warnings'].append(
                    f"Size mismatch: estimated {estimated_size_mb:.1f}MB vs "
                    f"expected {expected_size_mb}MB"
                )
                validation_result['size_check'] = False
        
        return validation_result
    
    def _save_to_parquet(
        self,
        data: pd.DataFrame,
        config: DatasetConfig,
        year: Optional[int] = None,
        etl_date: str = None
    ) -> Path:
        """
        Save DataFrame to parquet file with proper partitioning.
        
        Args:
            data: DataFrame to save
            config: Dataset configuration
            year: Year for partitioning (if applicable)
            etl_date: ETL date for partitioning
            
        Returns:
            Path to saved file
        """
        # Generate output path using configuration
        output_path_str = self.config_loader.get_output_path(
            config.name, year=year, etl_date=etl_date
        )
        output_path = Path(output_path_str)
        
        # Create directory structure
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure parquet options from config
        compression = config.output.get('compression', 'snappy')
        
        # Save to parquet
        data.to_parquet(
            output_path,
            compression=compression,
            index=False
        )
        
        self.logger.info(f"Saved {len(data)} rows to {output_path}")
        return output_path
    
    def extract_multiple_years(
        self,
        dataset_name: str,
        years: List[int],
        etl_date: Optional[str] = None,
        validate: bool = True,
        save_to_disk: bool = True
    ) -> Dict[int, Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Extract data for multiple years of a dataset.
        
        Args:
            dataset_name: Name of the dataset to extract
            years: List of years to extract
            etl_date: ETL date for partitioning (defaults to current date)
            validate: Whether to validate the extracted data
            save_to_disk: Whether to save extracted data to disk
            
        Returns:
            Dictionary mapping year to (data, metadata) tuples
        """
        config = self.config_loader.get_dataset_config(dataset_name)
        
        if not config.requires_year:
            raise ValueError(f"Dataset '{dataset_name}' doesn't support year-based extraction")
        
        results = {}
        total_years = len(years)
        
        for i, year in enumerate(years, 1):
            self.logger.info(f"Extracting year {year} ({i}/{total_years}) for {dataset_name}")
            
            try:
                data, metadata = self.extract_dataset(
                    dataset_name=dataset_name,
                    year=year,
                    etl_date=etl_date,
                    validate=validate,
                    save_to_disk=save_to_disk
                )
                results[year] = (data, metadata)
                
            except Exception as e:
                self.logger.error(f"Failed to extract {dataset_name} for year {year}: {str(e)}")
                # Continue with other years
                results[year] = (pd.DataFrame(), {'error': str(e)})
        
        return results
    
    def get_extraction_status(self, dataset_name: str, year: Optional[int] = None) -> Dict[str, Any]:
        """
        Get extraction status for a dataset (check if already extracted).
        
        Args:
            dataset_name: Name of the dataset
            year: Year to check (if applicable)
            
        Returns:
            Dictionary with extraction status information
        """
        config = self.config_loader.get_dataset_config(dataset_name)
        
        # Use current date as default ETL date for checking
        current_etl_date = datetime.now().strftime('%Y-%m-%d')
        output_path_str = self.config_loader.get_output_path(
            dataset_name, year=year, etl_date=current_etl_date
        )
        output_path = Path(output_path_str)
        
        status = {
            'dataset': dataset_name,
            'year': year,
            'exists': output_path.exists(),
            'path': str(output_path),
            'file_info': None
        }
        
        if output_path.exists():
            try:
                # Get file info
                stat = output_path.stat()
                status['file_info'] = {
                    'size_mb': round(stat.st_size / (1024 * 1024), 2),
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    'rows': None  # Would need to read parquet to get this
                }
                
                # Optionally read parquet metadata for row count
                try:
                    parquet_file = pq.ParquetFile(output_path)
                    status['file_info']['rows'] = parquet_file.metadata.num_rows
                except Exception:
                    pass  # Skip if unable to read metadata
                    
            except Exception as e:
                status['file_info'] = {'error': str(e)}
        
        return status
    
    def list_available_datasets(self) -> List[str]:
        """List all available datasets from configuration."""
        return self.config_loader.list_datasets()
    
    def get_dataset_info(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get comprehensive information about a dataset.
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Dictionary containing dataset information
        """
        config = self.config_loader.get_dataset_config(dataset_name)
        
        return {
            'name': config.name,
            'description': config.description,
            'function_name': config.function_name,
            'start_year': config.start_year,
            'requires_year': config.requires_year,
            'data_type': config.data_type,
            'partition_by': config.partition_by,
            'validation_rules': config.validation,
            'extraction_config': config.extraction,
            'output_config': config.output
        }