"""
Functional programming utilities for NFL CLI tool.

This module provides pure functional constructs including:
- Immutable result types
- Function composition utilities  
- Monadic error handling
- Type-safe pipeline construction
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar, Union
from functools import reduce, wraps
from pathlib import Path
import pandas as pd

# Type variables for generic functions
T = TypeVar('T')
R = TypeVar('R') 
E = TypeVar('E')

# Pipeline type for function composition
Pipeline = Callable[[T], 'CLIResult[R]']


@dataclass(frozen=True)
class CLIResult(Generic[T]):
    """
    Immutable result container for CLI operations.
    
    Provides monadic interface for chaining operations
    with automatic error propagation.
    """
    success: bool
    data: T | None
    error: str | None

    @staticmethod
    def ok(data: T) -> CLIResult[T]:
        """Create successful result."""
        return CLIResult(success=True, data=data, error=None)
    
    @staticmethod  
    def error(message: str) -> CLIResult[T]:
        """Create error result."""
        return CLIResult(success=False, data=None, error=message)
    
    def map(self, func: Callable[[T], R]) -> CLIResult[R]:
        """Apply function to data if result is successful."""
        if self.success and self.data is not None:
            try:
                return CLIResult.ok(func(self.data))
            except Exception as e:
                return CLIResult.error(str(e))
        return CLIResult.error(self.error or "Unknown error")
    
    def flat_map(self, func: Callable[[T], CLIResult[R]]) -> CLIResult[R]:
        """Monadic bind operation for chaining results."""
        if self.success and self.data is not None:
            return func(self.data)
        return CLIResult.error(self.error or "Unknown error")
    
    def or_else(self, default: T) -> T:
        """Get data or return default value."""
        return self.data if self.success and self.data is not None else default
    
    def unwrap(self) -> T:
        """Get data or raise exception if error."""
        if self.success and self.data is not None:
            return self.data
        raise ValueError(self.error or "Result contains no data")


@dataclass(frozen=True)
class ExtractionConfig:
    """Immutable configuration for data extraction."""
    dataset_name: str
    year: int | None
    validate: bool
    save_to_disk: bool
    output_path: Path | None


@dataclass(frozen=True)
class ExtractionMetadata:
    """Immutable metadata from extraction operation."""
    rows_extracted: int
    duration_seconds: float
    validation_passed: bool
    output_path: Path | None
    file_size_mb: float
    extraction_timestamp: str


@dataclass(frozen=True)
class ValidationReport:
    """Immutable data validation report."""
    dataset_name: str
    total_rows: int
    valid_rows: int
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    validation_passed: bool


@dataclass(frozen=True)
class DatasetInfo:
    """Immutable dataset information."""
    name: str
    description: str
    path: Path
    row_count: int
    file_size_mb: float
    last_modified: str


@dataclass(frozen=True)
class QueryParams:
    """Immutable query parameters."""
    model_name: str
    limit: int | None
    as_of_date: str | None
    show_schema: bool
    filters: dict[str, Any]


@dataclass(frozen=True)
class QueryResult:
    """Immutable query result."""
    data: pd.DataFrame
    row_count: int
    column_count: int
    execution_time_ms: float
    query_sql: str


@dataclass(frozen=True)
class MaterializationResult:
    """Immutable materialization result."""
    models_materialized: tuple[str, ...]
    success_count: int
    error_count: int
    total_duration_seconds: float
    errors: tuple[str, ...]


# Functional composition utilities
def compose(*functions: Callable) -> Callable:
    """
    Compose functions right-to-left.
    
    compose(f, g, h)(x) == f(g(h(x)))
    """
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(value: T, *functions: Callable) -> Any:
    """
    Pipe value through functions left-to-right.
    
    pipe(x, f, g, h) == h(g(f(x)))
    """
    return reduce(lambda acc, func: func(acc), functions, value)


def curry(func: Callable) -> Callable:
    """
    Curry a function to enable partial application.
    
    @curry
    def add(x, y): return x + y
    add_5 = add(5)  # Partial application
    """
    @wraps(func)
    def curried(*args, **kwargs):
        if len(args) + len(kwargs) >= func.__code__.co_argcount:
            return func(*args, **kwargs)
        return lambda *more_args, **more_kwargs: curried(
            *(args + more_args), **{**kwargs, **more_kwargs}
        )
    return curried


@curry
def map_result(func: Callable[[T], R], result: CLIResult[T]) -> CLIResult[R]:
    """Functional mapping over CLIResult."""
    return result.map(func)


@curry  
def chain_result(func: Callable[[T], CLIResult[R]], result: CLIResult[T]) -> CLIResult[R]:
    """Monadic chaining for CLIResult."""
    return result.flat_map(func)


def safe_call(func: Callable[..., T]) -> Callable[..., CLIResult[T]]:
    """
    Wrap function to return CLIResult, catching exceptions.
    
    @safe_call
    def risky_operation(x):
        return x / 0  # Will return CLIResult.error
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> CLIResult[T]:
        try:
            result = func(*args, **kwargs)
            return CLIResult.ok(result)
        except Exception as e:
            return CLIResult.error(f"{func.__name__} failed: {str(e)}")
    return wrapper


def maybe_call(func: Callable[[T], R], value: T | None) -> R | None:
    """Apply function to value if value is not None."""
    return func(value) if value is not None else None


def filter_results(predicate: Callable[[T], bool], results: list[CLIResult[T]]) -> list[CLIResult[T]]:
    """Filter successful results by predicate."""
    return [
        result for result in results 
        if result.success and result.data is not None and predicate(result.data)
    ]


def collect_errors(results: list[CLIResult[T]]) -> list[str]:
    """Collect all error messages from results."""
    return [
        result.error for result in results 
        if not result.success and result.error is not None
    ]


def all_successful(results: list[CLIResult[T]]) -> bool:
    """Check if all results are successful."""
    return all(result.success for result in results)


def first_success(results: list[CLIResult[T]]) -> CLIResult[T]:
    """Return first successful result, or error if none."""
    for result in results:
        if result.success:
            return result
    return CLIResult.error("No successful results found")


def combine_results(results: list[CLIResult[T]]) -> CLIResult[list[T]]:
    """Combine multiple results into single result with list of data."""
    if all_successful(results):
        data = [result.data for result in results if result.data is not None]
        return CLIResult.ok(data)
    
    errors = collect_errors(results)
    return CLIResult.error(f"Multiple errors: {'; '.join(errors)}")


# Immutable data utilities
def freeze_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Create immutable view of DataFrame."""
    # Create copy with immutable index and columns
    frozen_df = df.copy()
    frozen_df.flags.writeable = False
    return frozen_df


def merge_configs(**configs: dict[str, Any]) -> dict[str, Any]:
    """Merge configuration dictionaries immutably."""
    result = {}
    for config in configs.values():
        result = {**result, **config}
    return result


# Pipeline construction utilities
def build_pipeline(*steps: Callable[[T], CLIResult[Any]]) -> Pipeline:
    """Build pipeline from sequence of functions."""
    def pipeline(initial_value: T) -> CLIResult[Any]:
        result = CLIResult.ok(initial_value)
        for step in steps:
            result = result.flat_map(step)
            if not result.success:
                break
        return result
    return pipeline


def parallel_pipeline(steps: list[Callable[[T], CLIResult[R]]]) -> Callable[[T], CLIResult[list[R]]]:
    """Execute steps in parallel and combine results."""
    def pipeline(value: T) -> CLIResult[list[R]]:
        results = [step(value) for step in steps]
        return combine_results(results)
    return pipeline


# Validation utilities
def validate_path_exists(path: Path) -> CLIResult[Path]:
    """Validate that path exists."""
    if path.exists():
        return CLIResult.ok(path)
    return CLIResult.error(f"Path does not exist: {path}")


def validate_non_empty_dataframe(df: pd.DataFrame) -> CLIResult[pd.DataFrame]:
    """Validate DataFrame is not empty."""
    if len(df) > 0:
        return CLIResult.ok(df)
    return CLIResult.error("DataFrame is empty")


def validate_required_columns(df: pd.DataFrame, required_columns: list[str]) -> CLIResult[pd.DataFrame]:
    """Validate DataFrame has required columns."""
    missing = set(required_columns) - set(df.columns)
    if not missing:
        return CLIResult.ok(df)
    return CLIResult.error(f"Missing required columns: {missing}")


# Logging utilities for pure functions  
def log_operation(operation_name: str, func: Callable[[T], CLIResult[R]]) -> Callable[[T], CLIResult[R]]:
    """Add logging to pure function without side effects."""
    @wraps(func)
    def logged_func(value: T) -> CLIResult[R]:
        result = func(value)
        # Return result with operation context
        if result.success:
            return result
        else:
            enhanced_error = f"{operation_name}: {result.error}"
            return CLIResult.error(enhanced_error)
    return logged_func