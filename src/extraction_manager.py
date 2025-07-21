"""
Extraction Manager for incremental data extraction and status tracking.
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
from datetime import datetime, timedelta
import logging

from .config_loader import ConfigLoader
from .nfl_extractor import NFLDataExtractor


class ExtractionManager:
    """Manages incremental extraction and tracks extraction status."""
    
    def __init__(self, config_dir: Optional[Path] = None, state_file: Optional[Path] = None):
        """
        Initialize the Extraction Manager.
        
        Args:
            config_dir: Path to configuration directory
            state_file: Path to state file for tracking extractions
        """
        self.config_loader = ConfigLoader(config_dir)
        self.extractor = NFLDataExtractor(config_dir)
        
        # Default state file location
        if state_file is None:
            project_root = Path(__file__).parent.parent
            state_file = project_root / "data" / "extraction_state.json"
        
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        
        self._setup_logging()
        self.state = self._load_state()
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_state(self) -> Dict[str, Any]:
        """Load extraction state from file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Failed to load state file: {e}. Starting with empty state.")
        
        return {
            'last_updated': None,
            'extractions': {},  # dataset_name -> {year -> extraction_info}
            'metadata': {
                'version': '1.0',
                'created': datetime.now().isoformat()
            }
        }
    
    def _save_state(self) -> None:
        """Save extraction state to file."""
        self.state['last_updated'] = datetime.now().isoformat()
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save state file: {e}")
    
    def get_missing_extractions(self, dataset_name: str, years: List[int]) -> List[int]:
        """
        Get list of years that haven't been extracted for a dataset.
        
        Args:
            dataset_name: Name of the dataset
            years: List of years to check
            
        Returns:
            List of years that need to be extracted
        """
        if dataset_name not in self.state['extractions']:
            return years
        
        extracted_years = set(
            int(year) for year in self.state['extractions'][dataset_name].keys()
            if self.state['extractions'][dataset_name][year].get('status') == 'completed'
        )
        
        return [year for year in years if year not in extracted_years]
    
    def is_extraction_current(
        self, 
        dataset_name: str, 
        year: Optional[int] = None,
        max_age_days: int = 1
    ) -> bool:
        """
        Check if extraction is current (within max age).
        
        Args:
            dataset_name: Name of the dataset
            year: Year to check (None for non-year datasets)
            max_age_days: Maximum age in days to consider current
            
        Returns:
            True if extraction is current, False otherwise
        """
        if dataset_name not in self.state['extractions']:
            return False
        
        year_key = str(year) if year is not None else 'no_year'
        
        if year_key not in self.state['extractions'][dataset_name]:
            return False
        
        extraction_info = self.state['extractions'][dataset_name][year_key]
        
        if extraction_info.get('status') != 'completed':
            return False
        
        # Check age of extraction
        extraction_date = extraction_info.get('extraction_date')
        if not extraction_date:
            return False
        
        try:
            extraction_dt = datetime.fromisoformat(extraction_date.replace('Z', '+00:00'))
            age = datetime.now() - extraction_dt.replace(tzinfo=None)
            return age.days <= max_age_days
        except Exception:
            return False
    
    def extract_incremental(
        self,
        dataset_name: str,
        years: Optional[List[int]] = None,
        force_refresh: bool = False,
        max_age_days: int = 1
    ) -> Dict[str, Any]:
        """
        Perform incremental extraction for a dataset.
        
        Args:
            dataset_name: Name of the dataset to extract
            years: List of years to extract (None for non-year datasets)
            force_refresh: Force re-extraction even if current
            max_age_days: Maximum age to consider extraction current
            
        Returns:
            Dictionary with extraction summary
        """
        config = self.config_loader.get_dataset_config(dataset_name)
        
        extraction_summary = {
            'dataset': dataset_name,
            'started': datetime.now().isoformat(),
            'requires_year': config.requires_year,
            'years_requested': years,
            'years_extracted': [],
            'years_skipped': [],
            'errors': [],
            'total_rows': 0,
            'total_files': 0
        }
        
        try:
            if config.requires_year:
                if not years:
                    raise ValueError(f"Dataset '{dataset_name}' requires years to be specified")
                
                # Check which years need extraction
                if not force_refresh:
                    years_to_extract = [
                        year for year in years 
                        if not self.is_extraction_current(dataset_name, year, max_age_days)
                    ]
                    extraction_summary['years_skipped'] = [
                        year for year in years if year not in years_to_extract
                    ]
                else:
                    years_to_extract = years
                
                self.logger.info(
                    f"Extracting {len(years_to_extract)} years for {dataset_name}: {years_to_extract}"
                )
                
                # Extract each year
                for year in years_to_extract:
                    try:
                        data, metadata = self.extractor.extract_dataset(
                            dataset_name=dataset_name,
                            year=year,
                            validate=True,
                            save_to_disk=True
                        )
                        
                        # Update state
                        self._record_extraction(dataset_name, year, metadata, 'completed')
                        
                        extraction_summary['years_extracted'].append(year)
                        extraction_summary['total_rows'] += metadata['rows_extracted']
                        if metadata.get('output_path'):
                            extraction_summary['total_files'] += 1
                        
                    except Exception as e:
                        error_msg = f"Year {year}: {str(e)}"
                        extraction_summary['errors'].append(error_msg)
                        self._record_extraction(dataset_name, year, {'error': str(e)}, 'failed')
                        self.logger.error(f"Failed to extract {dataset_name} year {year}: {e}")
            
            else:
                # Non-year dataset
                if not force_refresh and self.is_extraction_current(dataset_name, None, max_age_days):
                    extraction_summary['years_skipped'] = ['no_year']
                    self.logger.info(f"Skipping {dataset_name} - extraction is current")
                else:
                    try:
                        data, metadata = self.extractor.extract_dataset(
                            dataset_name=dataset_name,
                            validate=True,
                            save_to_disk=True
                        )
                        
                        # Update state
                        self._record_extraction(dataset_name, None, metadata, 'completed')
                        
                        extraction_summary['years_extracted'] = ['no_year']
                        extraction_summary['total_rows'] = metadata['rows_extracted']
                        if metadata.get('output_path'):
                            extraction_summary['total_files'] = 1
                    
                    except Exception as e:
                        error_msg = f"Dataset extraction: {str(e)}"
                        extraction_summary['errors'].append(error_msg)
                        self._record_extraction(dataset_name, None, {'error': str(e)}, 'failed')
                        self.logger.error(f"Failed to extract {dataset_name}: {e}")
            
            extraction_summary['completed'] = datetime.now().isoformat()
            extraction_summary['success'] = len(extraction_summary['errors']) == 0
            
        except Exception as e:
            extraction_summary['errors'].append(f"Extraction setup failed: {str(e)}")
            extraction_summary['success'] = False
            self.logger.error(f"Extraction failed for {dataset_name}: {e}")
        
        return extraction_summary
    
    def _record_extraction(
        self,
        dataset_name: str,
        year: Optional[int],
        metadata: Dict[str, Any],
        status: str
    ) -> None:
        """Record extraction in state file."""
        if dataset_name not in self.state['extractions']:
            self.state['extractions'][dataset_name] = {}
        
        year_key = str(year) if year is not None else 'no_year'
        
        self.state['extractions'][dataset_name][year_key] = {
            'status': status,
            'extraction_date': datetime.now().isoformat(),
            'metadata': metadata
        }
        
        self._save_state()
    
    def get_extraction_summary(self, dataset_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get summary of extractions performed.
        
        Args:
            dataset_name: Specific dataset to summarize (None for all)
            
        Returns:
            Dictionary with extraction summary
        """
        if dataset_name:
            if dataset_name not in self.state['extractions']:
                return {'dataset': dataset_name, 'extractions': {}, 'summary': 'No extractions recorded'}
            
            extractions = self.state['extractions'][dataset_name]
        else:
            extractions = self.state['extractions']
        
        summary = {
            'total_datasets': len(extractions) if not dataset_name else 1,
            'total_extractions': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'last_extraction': None,
            'datasets': {}
        }
        
        datasets_to_process = [dataset_name] if dataset_name else extractions.keys()
        
        for ds_name in datasets_to_process:
            if ds_name not in self.state['extractions']:
                continue
                
            ds_extractions = self.state['extractions'][ds_name]
            ds_summary = {
                'total_years': len(ds_extractions),
                'completed': 0,
                'failed': 0,
                'last_extraction': None,
                'years': {}
            }
            
            for year_key, extraction_info in ds_extractions.items():
                summary['total_extractions'] += 1
                ds_summary['years'][year_key] = {
                    'status': extraction_info['status'],
                    'date': extraction_info['extraction_date'],
                    'rows': extraction_info['metadata'].get('rows_extracted', 0)
                }
                
                if extraction_info['status'] == 'completed':
                    summary['successful_extractions'] += 1
                    ds_summary['completed'] += 1
                else:
                    summary['failed_extractions'] += 1
                    ds_summary['failed'] += 1
                
                # Track last extraction
                extraction_date = extraction_info['extraction_date']
                if not summary['last_extraction'] or extraction_date > summary['last_extraction']:
                    summary['last_extraction'] = extraction_date
                if not ds_summary['last_extraction'] or extraction_date > ds_summary['last_extraction']:
                    ds_summary['last_extraction'] = extraction_date
            
            summary['datasets'][ds_name] = ds_summary
        
        return summary
    
    def cleanup_old_extractions(self, max_age_days: int = 30) -> Dict[str, Any]:
        """
        Clean up old extraction files and state entries.
        
        Args:
            max_age_days: Maximum age in days before cleanup
            
        Returns:
            Dictionary with cleanup summary
        """
        cleanup_summary = {
            'started': datetime.now().isoformat(),
            'files_deleted': 0,
            'bytes_freed': 0,
            'state_entries_cleaned': 0,
            'errors': []
        }
        
        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        
        for dataset_name, extractions in self.state['extractions'].copy().items():
            for year_key, extraction_info in extractions.copy().items():
                try:
                    extraction_date = datetime.fromisoformat(
                        extraction_info['extraction_date'].replace('Z', '+00:00')
                    )
                    
                    if extraction_date.replace(tzinfo=None) < cutoff_date:
                        # Try to delete file
                        output_path = extraction_info['metadata'].get('output_path')
                        if output_path:
                            file_path = Path(output_path)
                            if file_path.exists():
                                file_size = file_path.stat().st_size
                                file_path.unlink()
                                cleanup_summary['files_deleted'] += 1
                                cleanup_summary['bytes_freed'] += file_size
                        
                        # Remove from state
                        del self.state['extractions'][dataset_name][year_key]
                        cleanup_summary['state_entries_cleaned'] += 1
                
                except Exception as e:
                    cleanup_summary['errors'].append(
                        f"Failed to cleanup {dataset_name}:{year_key} - {str(e)}"
                    )
        
        # Remove empty dataset entries
        empty_datasets = [
            ds for ds, extractions in self.state['extractions'].items()
            if not extractions
        ]
        for ds in empty_datasets:
            del self.state['extractions'][ds]
        
        self._save_state()
        cleanup_summary['completed'] = datetime.now().isoformat()
        
        return cleanup_summary
    
    def get_recommended_years(self, dataset_name: str, limit: int = 5) -> List[int]:
        """
        Get recommended years to extract for a dataset.
        
        Args:
            dataset_name: Name of the dataset
            limit: Maximum number of years to recommend
            
        Returns:
            List of recommended years (most recent first)
        """
        config = self.config_loader.get_dataset_config(dataset_name)
        
        if not config.requires_year:
            return []
        
        current_year = datetime.now().year
        start_year = config.start_year
        
        # Get recent years, working backwards from last year
        # (current year data might not be complete)
        recommended_years = []
        year = current_year - 1
        
        while len(recommended_years) < limit and year >= start_year:
            recommended_years.append(year)
            year -= 1
        
        return recommended_years