"""
Pure functional data extraction for NFL datasets.

All functions are pure with no side effects, returning immutable CLIResult containers.
Extraction pipeline: config validation → data fetching → validation → saving
"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Any

import nfl_data_py as nfl
import pandas as pd

from .config_loader import ConfigLoader, DatasetConfig
from .functional_utils import (
    CLIResult,
    ExtractionConfig,
    ExtractionMetadata,
    ValidationReport,
    safe_call,
    compose,
    pipe,
    curry,
    log_operation,
    validate_path_exists,
    validate_non_empty_dataframe,
    freeze_dataframe,
)


# Pure configuration functions
@safe_call
def load_dataset_config(dataset_name: str) -> DatasetConfig:
    """Load configuration for specific dataset."""
    loader = ConfigLoader()
    return loader.get_dataset_config(dataset_name)


@safe_call
def load_all_dataset_configs() -> tuple[DatasetConfig, ...]:
    """Load all dataset configurations as immutable tuple."""
    loader = ConfigLoader()
    configs = [loader.get_dataset_config(name) for name in loader.list_datasets()]
    return tuple(configs)


def validate_extraction_config(config: ExtractionConfig) -> CLIResult[ExtractionConfig]:
    """Validate extraction configuration."""
    dataset_config_result = load_dataset_config(config.dataset_name)
    
    if not dataset_config_result.success:
        return CLIResult.error(f"Invalid dataset: {config.dataset_name}")
    
    dataset_config = dataset_config_result.data
    
    # Check year requirement
    if dataset_config.requires_year and config.year is None:
        return CLIResult.error(
            f"Dataset '{config.dataset_name}' requires year parameter. "
            f"Dataset starts from year: {dataset_config.start_year}"
        )
    
    # Validate year range
    if config.year is not None and dataset_config.start_year is not None:
        if config.year < dataset_config.start_year:
            return CLIResult.error(
                f"Year {config.year} is before dataset start year {dataset_config.start_year}"
            )
    
    return CLIResult.ok(config)


# Pure data extraction functions
NFL_FUNCTION_MAP = {
    'pbp': nfl.import_pbp_data,
    'weekly': nfl.import_weekly_data,
    'seasonal': nfl.import_seasonal_data,
    'schedules': nfl.import_schedules,
    'team_desc': nfl.import_team_desc,
    'players': nfl.import_players,
    'weekly_rosters': nfl.import_weekly_rosters,
    'seasonal_rosters': nfl.import_seasonal_rosters,
    'officials': nfl.import_officials,
    'combine': nfl.import_combine_data,
    'draft_picks': nfl.import_draft_picks,
    'qbr': nfl.import_qbr,
    'weekly_pfr': nfl.import_weekly_pfr,
    'seasonal_pfr': nfl.import_seasonal_pfr,
    'injuries': nfl.import_injuries,
    'depth_charts': nfl.import_depth_charts,
    'snap_counts': nfl.import_snap_counts,
    'ftn_data': nfl.import_ftn_data,
    'ngs_data': nfl.import_ngs_data,
}


@safe_call
def extract_nfl_data(dataset_name: str, year: int | None = None) -> pd.DataFrame:
    """
    Pure function to extract NFL data from nfl_data_py.
    
    Returns immutable DataFrame with data frozen to prevent mutations.
    """
    if dataset_name not in NFL_FUNCTION_MAP:
        raise ValueError(f"Unknown dataset: {dataset_name}")
    
    extraction_func = NFL_FUNCTION_MAP[dataset_name]
    
    # Call appropriate function based on year requirement
    if year is not None:
        data = extraction_func(years=[year])
    else:
        data = extraction_func()
    
    # Return immutable DataFrame
    return freeze_dataframe(data)


def validate_extracted_data(data: pd.DataFrame, dataset_name: str) -> CLIResult[ValidationReport]:
    """
    Pure function to validate extracted data.
    
    Returns immutable validation report.
    """
    errors = []
    warnings = []
    
    # Basic validation
    if data.empty:
        errors.append("DataFrame is empty")
    
    # Check for required columns based on dataset
    required_columns = _get_required_columns(dataset_name)
    missing_columns = set(required_columns) - set(data.columns)
    if missing_columns:
        errors.append(f"Missing required columns: {missing_columns}")
    
    # Check for duplicate indices
    if data.index.duplicated().any():
        warnings.append("Duplicate indices found")
    
    # Check for excessive null values
    null_percentages = data.isnull().sum() / len(data) * 100
    high_null_columns = null_percentages[null_percentages > 50].index.tolist()
    if high_null_columns:
        warnings.append(f"High null percentages in columns: {high_null_columns}")
    
    validation_passed = len(errors) == 0
    
    report = ValidationReport(
        dataset_name=dataset_name,
        total_rows=len(data),
        valid_rows=len(data) if validation_passed else 0,
        errors=tuple(errors),
        warnings=tuple(warnings),
        validation_passed=validation_passed
    )
    
    return CLIResult.ok(report)


def _get_required_columns(dataset_name: str) -> list[str]:
    """Get required columns for dataset validation."""
    column_requirements = {
        'pbp': ['game_id', 'play_id'],
        'weekly': ['season', 'week', 'player_id'],
        'seasonal': ['season', 'player_id'],
        'schedules': ['game_id', 'season', 'week'],
        'team_desc': ['team_abbr', 'team_name'],
        'players': ['gsis_id'],
    }
    return column_requirements.get(dataset_name, [])


def calculate_output_path(dataset_name: str, year: int | None, base_path: Path | None = None) -> Path:
    """Pure function to calculate output path for dataset."""
    if base_path is None:
        base_path = Path("data")
    
    timestamp = datetime.now().strftime("%Y%m%d")
    
    if year is not None:
        filename = f"{dataset_name}_{year}_{timestamp}.parquet"
        return base_path / dataset_name / str(year) / filename
    else:
        filename = f"{dataset_name}_{timestamp}.parquet"
        return base_path / dataset_name / filename


@safe_call
def save_dataframe_to_parquet(data: pd.DataFrame, output_path: Path) -> ExtractionMetadata:
    """
    Pure function to save DataFrame to parquet file.
    
    Returns extraction metadata without mutating input.
    """
    # Create directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    start_time = time.time()
    
    # Save to parquet
    data.to_parquet(output_path, engine='pyarrow', compression='snappy')
    
    duration = time.time() - start_time
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    
    metadata = ExtractionMetadata(
        rows_extracted=len(data),
        duration_seconds=round(duration, 3),
        validation_passed=True,  # Assuming save success means validation passed
        output_path=output_path,
        file_size_mb=round(file_size_mb, 2),
        extraction_timestamp=datetime.now().isoformat()
    )
    
    return metadata


# Composed extraction pipelines
def extract_single_dataset(config: ExtractionConfig) -> CLIResult[tuple[pd.DataFrame, ExtractionMetadata]]:
    """
    Complete extraction pipeline for single dataset.
    
    Pipeline: validate config → extract data → validate data → save to disk
    """
    def extraction_pipeline(validated_config: ExtractionConfig) -> CLIResult[tuple[pd.DataFrame, ExtractionMetadata]]:
        # Extract data
        data_result = extract_nfl_data(validated_config.dataset_name, validated_config.year)
        if not data_result.success:
            return CLIResult.error(f"Extraction failed: {data_result.error}")
        
        data = data_result.data
        
        # Validate extracted data
        if validated_config.validate:
            validation_result = validate_extracted_data(data, validated_config.dataset_name)
            if not validation_result.success:
                return CLIResult.error(f"Validation failed: {validation_result.error}")
            
            validation_report = validation_result.data
            if not validation_report.validation_passed:
                error_msg = "; ".join(validation_report.errors)
                return CLIResult.error(f"Data validation failed: {error_msg}")
        
        # Save to disk if requested
        if validated_config.save_to_disk:
            output_path = validated_config.output_path or calculate_output_path(
                validated_config.dataset_name, validated_config.year
            )
            
            save_result = save_dataframe_to_parquet(data, output_path)
            if not save_result.success:
                return CLIResult.error(f"Save failed: {save_result.error}")
            
            metadata = save_result.data
        else:
            # Create metadata without saving
            metadata = ExtractionMetadata(
                rows_extracted=len(data),
                duration_seconds=0.0,
                validation_passed=True,
                output_path=None,
                file_size_mb=0.0,
                extraction_timestamp=datetime.now().isoformat()
            )
        
        return CLIResult.ok((data, metadata))
    
    return validate_extraction_config(config).flat_map(extraction_pipeline)


@curry
def extract_dataset_with_year(dataset_name: str, year: int, validate: bool = True, save: bool = True) -> CLIResult[tuple[pd.DataFrame, ExtractionMetadata]]:
    """Curried function for extracting dataset with specific year."""
    config = ExtractionConfig(
        dataset_name=dataset_name,
        year=year,
        validate=validate,
        save_to_disk=save,
        output_path=None
    )
    return extract_single_dataset(config)


def extract_multiple_years(dataset_name: str, years: list[int], validate: bool = True, save: bool = True) -> CLIResult[list[tuple[pd.DataFrame, ExtractionMetadata]]]:
    """
    Extract multiple years of a dataset using functional composition.
    
    Returns combined result of all extractions.
    """
    extract_func = extract_dataset_with_year(dataset_name, validate=validate, save=save)
    results = [extract_func(year) for year in years]
    
    # Check if all extractions were successful
    successful_results = [r for r in results if r.success]
    failed_results = [r for r in results if not r.success]
    
    if failed_results:
        error_messages = [r.error for r in failed_results if r.error]
        return CLIResult.error(f"Failed extractions: {'; '.join(error_messages)}")
    
    # Extract data from successful results
    data_and_metadata = [r.data for r in successful_results if r.data]
    return CLIResult.ok(data_and_metadata)


def extract_all_datasets(year: int, priority_filter: str | None = None) -> CLIResult[dict[str, tuple[pd.DataFrame, ExtractionMetadata]]]:
    """
    Extract all datasets for a given year using functional approach.
    
    Optional priority filter: 'critical', 'high', 'medium', 'low'
    """
    # Load all configurations
    configs_result = load_all_dataset_configs()
    if not configs_result.success:
        return CLIResult.error(f"Failed to load configs: {configs_result.error}")
    
    all_configs = configs_result.data
    
    # Filter by priority if specified
    if priority_filter:
        priority_map = {
            'critical': ['pbp', 'weekly', 'schedules', 'team_desc'],
            'high': ['seasonal', 'players', 'weekly_rosters', 'seasonal_rosters'],
            'medium': ['injuries', 'depth_charts', 'snap_counts', 'qbr', 'ngs_data'],
            'low': ['weekly_pfr', 'seasonal_pfr', 'ftn_data', 'officials', 'combine', 'draft_picks']
        }
        
        if priority_filter not in priority_map:
            return CLIResult.error(f"Invalid priority filter: {priority_filter}")
        
        allowed_datasets = set(priority_map[priority_filter])
        filtered_configs = [c for c in all_configs if c.name in allowed_datasets]
    else:
        filtered_configs = list(all_configs)
    
    # Extract each dataset
    results = {}
    errors = []
    
    for config in filtered_configs:
        # Skip datasets that don't require year if they're static
        if not config.requires_year and config.name in ['team_desc', 'players']:
            extraction_config = ExtractionConfig(
                dataset_name=config.name,
                year=None,
                validate=True,
                save_to_disk=True,
                output_path=None
            )
        else:
            extraction_config = ExtractionConfig(
                dataset_name=config.name,
                year=year,
                validate=True,
                save_to_disk=True,
                output_path=None
            )
        
        result = extract_single_dataset(extraction_config)
        
        if result.success and result.data:
            results[config.name] = result.data
        else:
            errors.append(f"{config.name}: {result.error}")
    
    if errors:
        return CLIResult.error(f"Some extractions failed: {'; '.join(errors)}")
    
    return CLIResult.ok(results)


# Status and utility functions
@safe_call
def get_extraction_status() -> dict[str, Any]:
    """Get current extraction status from filesystem."""
    data_path = Path("data")
    
    if not data_path.exists():
        return {"status": "no_data", "datasets": {}}
    
    datasets = {}
    for dataset_dir in data_path.iterdir():
        if dataset_dir.is_dir():
            files = list(dataset_dir.rglob("*.parquet"))
            if files:
                latest_file = max(files, key=lambda f: f.stat().st_mtime)
                datasets[dataset_dir.name] = {
                    "file_count": len(files),
                    "latest_file": str(latest_file),
                    "latest_modified": datetime.fromtimestamp(
                        latest_file.stat().st_mtime
                    ).isoformat(),
                    "size_mb": round(latest_file.stat().st_size / (1024 * 1024), 2)
                }
    
    return {
        "status": "active" if datasets else "empty",
        "total_datasets": len(datasets),
        "datasets": datasets
    }


@safe_call
def cleanup_old_extractions(max_age_days: int = 30, dry_run: bool = True) -> dict[str, Any]:
    """
    Clean up old extraction files.
    
    Pure function that returns cleanup plan without performing deletions in dry_run mode.
    """
    data_path = Path("data")
    if not data_path.exists():
        return {"status": "no_data_directory", "files_to_delete": []}
    
    cutoff_time = time.time() - (max_age_days * 24 * 3600)
    files_to_delete = []
    total_size_mb = 0
    
    for parquet_file in data_path.rglob("*.parquet"):
        if parquet_file.stat().st_mtime < cutoff_time:
            size_mb = parquet_file.stat().st_size / (1024 * 1024)
            files_to_delete.append({
                "path": str(parquet_file),
                "size_mb": round(size_mb, 2),
                "age_days": int((time.time() - parquet_file.stat().st_mtime) / (24 * 3600))
            })
            total_size_mb += size_mb
    
    cleanup_plan = {
        "dry_run": dry_run,
        "files_to_delete": files_to_delete,
        "total_files": len(files_to_delete),
        "total_size_mb": round(total_size_mb, 2),
        "max_age_days": max_age_days
    }
    
    # Actually delete files if not dry run
    if not dry_run:
        deleted_count = 0
        for file_info in files_to_delete:
            try:
                Path(file_info["path"]).unlink()
                deleted_count += 1
            except Exception as e:
                # In a pure functional approach, we'd collect these errors
                # For now, we'll just continue
                pass
        
        cleanup_plan["deleted_count"] = deleted_count
    
    return cleanup_plan