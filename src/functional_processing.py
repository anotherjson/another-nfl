"""
Pure functional data processing for NFL parquet files.

All functions are pure with no side effects, returning immutable CLIResult containers.
Processing pipeline: file reading → validation → catalog generation
"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .functional_utils import (
    CLIResult,
    DatasetInfo,
    ValidationReport,
    safe_call,
    compose,
    pipe,
    curry,
    validate_path_exists,
    validate_non_empty_dataframe,
    validate_required_columns,
    freeze_dataframe,
)


# Pure file reading functions
@safe_call
def read_parquet_file(file_path: Path) -> pd.DataFrame:
    """
    Pure function to read parquet file.
    
    Returns immutable DataFrame with data frozen to prevent mutations.
    """
    data = pd.read_parquet(file_path, engine='pyarrow')
    return freeze_dataframe(data)


@safe_call
def get_file_info(file_path: Path) -> dict[str, Any]:
    """Pure function to get file metadata."""
    stat = file_path.stat()
    return {
        "size_bytes": stat.st_size,
        "size_mb": round(stat.st_size / (1024 * 1024), 2),
        "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "created_time": datetime.fromtimestamp(stat.st_ctime).isoformat(),
    }


def inspect_dataframe_schema(df: pd.DataFrame) -> dict[str, Any]:
    """
    Pure function to inspect DataFrame schema.
    
    Returns immutable schema information.
    """
    return {
        "shape": df.shape,
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "null_counts": df.isnull().sum().to_dict(),
        "memory_usage_mb": round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
        "index_type": str(type(df.index).__name__),
    }


def sample_dataframe(df: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    """
    Pure function to sample DataFrame.
    
    Returns immutable sample without modifying original.
    """
    if len(df) <= limit:
        return freeze_dataframe(df.copy())
    
    # Take first few rows, last few rows, and some from middle
    if limit >= 6:
        head_rows = limit // 3
        tail_rows = limit // 3
        middle_rows = limit - head_rows - tail_rows
        middle_start = len(df) // 2 - middle_rows // 2
        
        sample_df = pd.concat([
            df.head(head_rows),
            df.iloc[middle_start:middle_start + middle_rows],
            df.tail(tail_rows)
        ])
    else:
        sample_df = df.head(limit)
    
    return freeze_dataframe(sample_df)


# Data validation functions
def validate_nfl_data_structure(df: pd.DataFrame, dataset_name: str | None = None) -> CLIResult[ValidationReport]:
    """
    Pure function to validate NFL data structure.
    
    Returns immutable validation report.
    """
    errors = []
    warnings = []
    
    # Basic structure validation
    if df.empty:
        errors.append("DataFrame is empty")
        return CLIResult.ok(ValidationReport(
            dataset_name=dataset_name or "unknown",
            total_rows=0,
            valid_rows=0,
            errors=tuple(errors),
            warnings=tuple(warnings),
            validation_passed=False
        ))
    
    # Check for common NFL data patterns
    common_nfl_columns = {
        'season', 'week', 'game_id', 'player_id', 'team', 'team_abbr',
        'position', 'player_name', 'home_team', 'away_team'
    }
    
    present_nfl_columns = set(df.columns) & common_nfl_columns
    if not present_nfl_columns:
        warnings.append("No common NFL columns detected - may not be NFL data")
    
    # Check data types
    if 'season' in df.columns:
        if not pd.api.types.is_numeric_dtype(df['season']):
            warnings.append("Season column is not numeric")
        else:
            season_range = df['season'].dropna()
            if len(season_range) > 0:
                min_season, max_season = season_range.min(), season_range.max()
                if min_season < 1970 or max_season > datetime.now().year + 1:
                    warnings.append(f"Unusual season range: {min_season}-{max_season}")
    
    if 'week' in df.columns:
        if pd.api.types.is_numeric_dtype(df['week']):
            week_range = df['week'].dropna()
            if len(week_range) > 0:
                min_week, max_week = week_range.min(), week_range.max()
                if min_week < 1 or max_week > 22:
                    warnings.append(f"Unusual week range: {min_week}-{max_week}")
    
    # Check for excessive null values
    null_percentages = df.isnull().sum() / len(df) * 100
    high_null_columns = null_percentages[null_percentages > 75].index.tolist()
    if high_null_columns:
        warnings.append(f"Very high null percentages (>75%) in columns: {high_null_columns}")
    
    # Check for duplicate rows
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        warnings.append(f"Found {duplicate_count} duplicate rows ({duplicate_count/len(df)*100:.1f}%)")
    
    # Validation passes if no errors
    validation_passed = len(errors) == 0
    valid_rows = len(df) if validation_passed else len(df) - df.isnull().all(axis=1).sum()
    
    report = ValidationReport(
        dataset_name=dataset_name or "unknown",
        total_rows=len(df),
        valid_rows=valid_rows,
        errors=tuple(errors),
        warnings=tuple(warnings),
        validation_passed=validation_passed
    )
    
    return CLIResult.ok(report)


def detect_dataset_type(df: pd.DataFrame) -> str:
    """
    Pure function to detect NFL dataset type from DataFrame structure.
    
    Returns best guess of dataset type.
    """
    columns = set(df.columns)
    
    # Play-by-play data
    if {'play_id', 'game_id', 'down', 'yards_gained'}.issubset(columns):
        return 'pbp'
    
    # Weekly player stats
    if {'week', 'player_id', 'targets', 'receptions'}.issubset(columns):
        return 'weekly'
    
    # Seasonal stats
    if {'season', 'player_id'}.issubset(columns) and 'week' not in columns:
        return 'seasonal'
    
    # Schedules
    if {'game_id', 'home_team', 'away_team', 'season', 'week'}.issubset(columns):
        return 'schedules'
    
    # Team descriptions
    if {'team_abbr', 'team_name'}.issubset(columns) and len(df) <= 32:
        return 'team_desc'
    
    # Players
    if {'gsis_id', 'position'}.issubset(columns) and 'season' not in columns:
        return 'players'
    
    # Rosters
    if {'player_id', 'team'}.issubset(columns):
        if 'week' in columns:
            return 'weekly_rosters'
        else:
            return 'seasonal_rosters'
    
    # Injuries
    if {'report_status', 'player_id'}.issubset(columns):
        return 'injuries'
    
    # QBR
    if {'qbr_total', 'player_id'}.issubset(columns):
        return 'qbr'
    
    # Next Gen Stats
    if {'avg_time_to_throw', 'avg_completed_air_yards'}.issubset(columns):
        return 'ngs_data'
    
    # Snap counts
    if {'offense_snaps', 'defense_snaps'}.issubset(columns):
        return 'snap_counts'
    
    return 'unknown'


# Catalog generation functions
def scan_data_directory(base_path: Path = Path("data")) -> CLIResult[tuple[DatasetInfo, ...]]:
    """
    Pure function to scan data directory and generate catalog.
    
    Returns immutable tuple of dataset information.
    """
    if not base_path.exists():
        return CLIResult.error(f"Data directory does not exist: {base_path}")
    
    dataset_infos = []
    
    try:
        for dataset_dir in base_path.iterdir():
            if not dataset_dir.is_dir():
                continue
            
            # Find all parquet files in this dataset directory
            parquet_files = list(dataset_dir.rglob("*.parquet"))
            if not parquet_files:
                continue
            
            # Get info for most recent file
            latest_file = max(parquet_files, key=lambda f: f.stat().st_mtime)
            file_info_result = get_file_info(latest_file)
            
            if not file_info_result.success:
                continue
            
            file_info = file_info_result.data
            
            # Try to read and get row count
            try:
                read_result = read_parquet_file(latest_file)
                if read_result.success and read_result.data is not None:
                    row_count = len(read_result.data)
                else:
                    row_count = 0
            except Exception:
                row_count = 0
            
            dataset_info = DatasetInfo(
                name=dataset_dir.name,
                description=f"NFL {dataset_dir.name} data",
                path=latest_file,
                row_count=row_count,
                file_size_mb=file_info["size_mb"],
                last_modified=file_info["modified_time"]
            )
            
            dataset_infos.append(dataset_info)
    
    except Exception as e:
        return CLIResult.error(f"Error scanning directory: {str(e)}")
    
    # Sort by name for consistent ordering
    sorted_infos = sorted(dataset_infos, key=lambda x: x.name)
    return CLIResult.ok(tuple(sorted_infos))


def generate_catalog_summary(dataset_infos: tuple[DatasetInfo, ...]) -> dict[str, Any]:
    """
    Pure function to generate catalog summary statistics.
    
    Returns immutable summary dictionary.
    """
    if not dataset_infos:
        return {
            "total_datasets": 0,
            "total_files": 0,
            "total_size_mb": 0.0,
            "total_rows": 0,
            "datasets": []
        }
    
    total_size_mb = sum(info.file_size_mb for info in dataset_infos)
    total_rows = sum(info.row_count for info in dataset_infos)
    
    # Group by dataset type if detectable
    dataset_list = []
    for info in dataset_infos:
        dataset_list.append({
            "name": info.name,
            "path": str(info.path),
            "rows": info.row_count,
            "size_mb": info.file_size_mb,
            "last_modified": info.last_modified
        })
    
    return {
        "total_datasets": len(dataset_infos),
        "total_files": len(dataset_infos),  # One file per dataset in this summary
        "total_size_mb": round(total_size_mb, 2),
        "total_rows": total_rows,
        "datasets": dataset_list
    }


# Composed processing pipelines
def process_parquet_file(file_path: Path, limit: int = 10, validate: bool = True) -> CLIResult[dict[str, Any]]:
    """
    Complete processing pipeline for single parquet file.
    
    Pipeline: validate path → read file → validate data → sample data → inspect schema
    """
    def processing_pipeline(validated_path: Path) -> CLIResult[dict[str, Any]]:
        # Read file
        read_result = read_parquet_file(validated_path)
        if not read_result.success:
            return CLIResult.error(f"Failed to read file: {read_result.error}")
        
        data = read_result.data
        
        # Validate data if requested
        validation_report = None
        if validate:
            dataset_type = detect_dataset_type(data)
            validation_result = validate_nfl_data_structure(data, dataset_type)
            if not validation_result.success:
                return CLIResult.error(f"Validation failed: {validation_result.error}")
            validation_report = validation_result.data
        
        # Sample data
        sample_data = sample_dataframe(data, limit)
        
        # Inspect schema
        schema_info = inspect_dataframe_schema(data)
        
        # Get file info
        file_info_result = get_file_info(validated_path)
        if not file_info_result.success:
            return CLIResult.error(f"Failed to get file info: {file_info_result.error}")
        
        file_info = file_info_result.data
        
        # Combine all information
        result = {
            "file_path": str(validated_path),
            "file_info": file_info,
            "schema": schema_info,
            "sample_data": sample_data,
            "detected_type": detect_dataset_type(data),
            "validation_report": validation_report
        }
        
        return CLIResult.ok(result)
    
    return validate_path_exists(file_path).flat_map(processing_pipeline)


@curry
def process_dataset_files(dataset_name: str, base_path: Path = Path("data")) -> CLIResult[list[dict[str, Any]]]:
    """
    Process all files for a specific dataset.
    
    Returns list of processing results for all files.
    """
    dataset_path = base_path / dataset_name
    if not dataset_path.exists():
        return CLIResult.error(f"Dataset directory does not exist: {dataset_path}")
    
    parquet_files = list(dataset_path.rglob("*.parquet"))
    if not parquet_files:
        return CLIResult.error(f"No parquet files found in: {dataset_path}")
    
    results = []
    errors = []
    
    for file_path in parquet_files:
        result = process_parquet_file(file_path, limit=5, validate=False)
        if result.success and result.data:
            results.append(result.data)
        else:
            errors.append(f"{file_path.name}: {result.error}")
    
    if errors and not results:
        return CLIResult.error(f"All files failed to process: {'; '.join(errors)}")
    
    # Return successful results even if some failed
    return CLIResult.ok(results)


def validate_all_datasets(base_path: Path = Path("data")) -> CLIResult[dict[str, ValidationReport]]:
    """
    Validate all datasets in data directory using functional composition.
    
    Returns dictionary mapping dataset names to validation reports.
    """
    catalog_result = scan_data_directory(base_path)
    if not catalog_result.success:
        return CLIResult.error(f"Failed to scan directory: {catalog_result.error}")
    
    dataset_infos = catalog_result.data
    validation_results = {}
    errors = []
    
    for info in dataset_infos:
        read_result = read_parquet_file(info.path)
        if not read_result.success:
            errors.append(f"{info.name}: Failed to read - {read_result.error}")
            continue
        
        data = read_result.data
        validation_result = validate_nfl_data_structure(data, info.name)
        
        if validation_result.success and validation_result.data:
            validation_results[info.name] = validation_result.data
        else:
            errors.append(f"{info.name}: Validation failed - {validation_result.error}")
    
    if errors and not validation_results:
        return CLIResult.error(f"All validations failed: {'; '.join(errors)}")
    
    return CLIResult.ok(validation_results)


# Utility functions
def get_processing_summary(base_path: Path = Path("data")) -> CLIResult[dict[str, Any]]:
    """
    Get comprehensive processing summary of all data.
    
    Combines catalog information with validation results.
    """
    # Get catalog
    catalog_result = scan_data_directory(base_path)
    if not catalog_result.success:
        return CLIResult.error(f"Failed to generate catalog: {catalog_result.error}")
    
    dataset_infos = catalog_result.data
    catalog_summary = generate_catalog_summary(dataset_infos)
    
    # Get validation summary
    validation_result = validate_all_datasets(base_path)
    
    validation_summary = {}
    if validation_result.success and validation_result.data:
        validations = validation_result.data
        validation_summary = {
            "total_validated": len(validations),
            "passed_validation": sum(1 for v in validations.values() if v.validation_passed),
            "failed_validation": sum(1 for v in validations.values() if not v.validation_passed),
            "total_warnings": sum(len(v.warnings) for v in validations.values()),
            "total_errors": sum(len(v.errors) for v in validations.values())
        }
    
    summary = {
        "catalog": catalog_summary,
        "validation": validation_summary,
        "timestamp": datetime.now().isoformat()
    }
    
    return CLIResult.ok(summary)