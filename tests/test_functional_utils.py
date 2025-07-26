"""
Property-based tests for functional utilities.

Tests the core functional programming constructs using Hypothesis for property-based testing.
"""

from __future__ import annotations

import pytest
from hypothesis import given, strategies as st, assume
from hypothesis.strategies import composite
import pandas as pd
from pathlib import Path

from src.functional_utils import (
    CLIResult,
    ExtractionConfig,
    QueryParams,
    ValidationReport,
    ExtractionMetadata,
    compose,
    pipe,
    curry,
    safe_call,
    map_result,
    chain_result,
    combine_results,
    all_successful,
    first_success,
    collect_errors,
    freeze_dataframe,
)


# Custom strategies for testing
@composite
def cli_result_strategy(draw, data_strategy=st.text()):
    """Generate CLIResult instances for testing."""
    success = draw(st.booleans())
    if success:
        data = draw(data_strategy)
        return CLIResult.ok(data)
    else:
        error = draw(st.text(min_size=1))
        return CLIResult.error(error)


@composite
def extraction_config_strategy(draw):
    """Generate valid ExtractionConfig instances."""
    dataset_name = draw(st.sampled_from(['pbp', 'weekly', 'seasonal', 'team_desc']))
    year = draw(st.one_of(st.none(), st.integers(min_value=1999, max_value=2024)))
    validate = draw(st.booleans())
    save_to_disk = draw(st.booleans())
    output_path = draw(st.one_of(st.none(), st.builds(Path, st.text())))
    
    return ExtractionConfig(
        dataset_name=dataset_name,
        year=year,
        validate=validate,
        save_to_disk=save_to_disk,
        output_path=output_path
    )


@composite
def query_params_strategy(draw):
    """Generate valid QueryParams instances."""
    model_name = draw(st.sampled_from(['stg_pbp', 'stg_weekly', 'stg_team_desc']))
    limit = draw(st.one_of(st.none(), st.integers(min_value=1, max_value=1000)))
    as_of_date = draw(st.one_of(st.none(), st.text()))
    show_schema = draw(st.booleans())
    filters = draw(st.dictionaries(st.text(), st.one_of(st.text(), st.integers())))
    
    return QueryParams(
        model_name=model_name,
        limit=limit,
        as_of_date=as_of_date,
        show_schema=show_schema,
        filters=filters
    )


class TestCLIResult:
    """Test CLIResult monad properties."""
    
    @given(st.text())
    def test_ok_constructor_properties(self, data: str):
        """Test that ok constructor creates valid successful result."""
        result = CLIResult.ok(data)
        
        assert result.success is True
        assert result.data == data
        assert result.error is None
    
    @given(st.text(min_size=1))
    def test_error_constructor_properties(self, error: str):
        """Test that error constructor creates valid error result."""
        result = CLIResult.error(error)
        
        assert result.success is False
        assert result.data is None
        assert result.error == error
    
    @given(st.text(), st.text())
    def test_map_preserves_success(self, data: str, suffix: str):
        """Test that map preserves successful results."""
        result = CLIResult.ok(data)
        mapped = result.map(lambda x: x + suffix)
        
        assert mapped.success is True
        assert mapped.data == data + suffix
        assert mapped.error is None
    
    @given(st.text(min_size=1), st.text())
    def test_map_preserves_error(self, error: str, suffix: str):
        """Test that map preserves error results."""
        result = CLIResult.error(error)
        mapped = result.map(lambda x: x + suffix)
        
        assert mapped.success is False
        assert mapped.data is None
        assert mapped.error == error
    
    @given(st.text())
    def test_flat_map_with_success(self, data: str):
        """Test flat_map with successful result."""
        result = CLIResult.ok(data)
        flat_mapped = result.flat_map(lambda x: CLIResult.ok(x.upper()))
        
        assert flat_mapped.success is True
        assert flat_mapped.data == data.upper()
    
    @given(st.text(min_size=1))
    def test_flat_map_with_error(self, error: str):
        """Test flat_map with error result."""
        result = CLIResult.error(error)
        flat_mapped = result.flat_map(lambda x: CLIResult.ok(x.upper()))
        
        assert flat_mapped.success is False
        assert flat_mapped.error == error
    
    @given(st.text(), st.text())
    def test_or_else_with_success(self, data: str, default: str):
        """Test or_else with successful result returns data."""
        result = CLIResult.ok(data)
        value = result.or_else(default)
        
        assert value == data
    
    @given(st.text(min_size=1), st.text())
    def test_or_else_with_error(self, error: str, default: str):
        """Test or_else with error result returns default."""
        result = CLIResult.error(error)
        value = result.or_else(default)
        
        assert value == default
    
    @given(st.text())
    def test_unwrap_with_success(self, data: str):
        """Test unwrap with successful result returns data."""
        result = CLIResult.ok(data)
        value = result.unwrap()
        
        assert value == data
    
    @given(st.text(min_size=1))
    def test_unwrap_with_error_raises(self, error: str):
        """Test unwrap with error result raises exception."""
        result = CLIResult.error(error)
        
        with pytest.raises(ValueError, match=error):
            result.unwrap()


class TestFunctionalUtilities:
    """Test functional utility functions."""
    
    @given(st.lists(st.integers(), min_size=1))
    def test_compose_function_composition(self, numbers: list[int]):
        """Test that compose works correctly with multiple functions."""
        add_one = lambda x: x + 1
        multiply_two = lambda x: x * 2
        subtract_three = lambda x: x - 3
        
        # compose applies right to left: subtract_three(multiply_two(add_one(x)))
        composed = compose(subtract_three, multiply_two, add_one)
        
        for num in numbers:
            expected = subtract_three(multiply_two(add_one(num)))
            assert composed(num) == expected
    
    @given(st.integers(), st.lists(st.integers(), min_size=0, max_size=5))
    def test_pipe_function_pipeline(self, initial: int, increments: list[int]):
        """Test that pipe applies functions left to right."""
        functions = [lambda x, inc=inc: x + inc for inc in increments]
        
        result = pipe(initial, *functions)
        
        # Calculate expected result
        expected = initial
        for inc in increments:
            expected += inc
        
        assert result == expected
    
    @given(st.integers(), st.integers())
    def test_curry_partial_application(self, x: int, y: int):
        """Test that curry enables partial application."""
        @curry
        def add(a: int, b: int) -> int:
            return a + b
        
        add_x = add(x)  # Partial application
        result = add_x(y)
        
        assert result == x + y
        assert add(x)(y) == x + y  # Alternative syntax
        assert add(x, y) == x + y  # Full application
    
    @given(st.text())
    def test_safe_call_with_success(self, data: str):
        """Test safe_call with function that succeeds."""
        @safe_call
        def identity(x):
            return x
        
        result = identity(data)
        
        assert result.success is True
        assert result.data == data
        assert result.error is None
    
    def test_safe_call_with_exception(self):
        """Test safe_call with function that raises exception."""
        @safe_call
        def divide_by_zero(x):
            return x / 0
        
        result = divide_by_zero(10)
        
        assert result.success is False
        assert result.data is None
        assert "divide_by_zero failed:" in result.error
    
    @given(cli_result_strategy(st.integers()))
    def test_map_result_function(self, result: CLIResult[int]):
        """Test map_result curried function."""
        double = lambda x: x * 2
        mapped = map_result(double)(result)
        
        if result.success:
            assert mapped.success is True
            assert mapped.data == result.data * 2
        else:
            assert mapped.success is False
            assert mapped.error == result.error
    
    @given(cli_result_strategy(st.integers()))
    def test_chain_result_function(self, result: CLIResult[int]):
        """Test chain_result curried function."""
        double_and_wrap = lambda x: CLIResult.ok(x * 2)
        chained = chain_result(double_and_wrap)(result)
        
        if result.success:
            assert chained.success is True
            assert chained.data == result.data * 2
        else:
            assert chained.success is False
            assert chained.error == result.error


class TestResultCombinators:
    """Test result combination functions."""
    
    @given(st.lists(cli_result_strategy(st.integers()), min_size=1))
    def test_all_successful_property(self, results: list[CLIResult[int]]):
        """Test all_successful returns True only when all results are successful."""
        expected = all(r.success for r in results)
        assert all_successful(results) == expected
    
    @given(st.lists(cli_result_strategy(st.integers()), min_size=1))
    def test_collect_errors_property(self, results: list[CLIResult[int]]):
        """Test collect_errors returns all error messages."""
        expected_errors = [r.error for r in results if not r.success and r.error is not None]
        actual_errors = collect_errors(results)
        
        assert actual_errors == expected_errors
    
    @given(st.lists(cli_result_strategy(st.integers()), min_size=1))
    def test_first_success_property(self, results: list[CLIResult[int]]):
        """Test first_success returns first successful result."""
        first_success_result = first_success(results)
        
        successful_results = [r for r in results if r.success]
        
        if successful_results:
            assert first_success_result.success is True
            assert first_success_result.data == successful_results[0].data
        else:
            assert first_success_result.success is False
    
    @given(st.lists(cli_result_strategy(st.integers()), min_size=1))
    def test_combine_results_property(self, results: list[CLIResult[int]]):
        """Test combine_results combines all successful data."""
        combined = combine_results(results)
        
        if all_successful(results):
            assert combined.success is True
            expected_data = [r.data for r in results if r.data is not None]
            assert combined.data == expected_data
        else:
            assert combined.success is False


class TestDataFrameUtilities:
    """Test DataFrame-related utilities."""
    
    @given(st.integers(min_value=1, max_value=100), st.integers(min_value=1, max_value=10))
    def test_freeze_dataframe_immutability(self, rows: int, cols: int):
        """Test that freeze_dataframe creates immutable DataFrame."""
        # Create test DataFrame
        data = {f'col_{i}': list(range(rows)) for i in range(cols)}
        df = pd.DataFrame(data)
        
        frozen_df = freeze_dataframe(df)
        
        # Test that the frozen DataFrame is not writable
        assert not frozen_df.flags.writeable
        
        # Test that original DataFrame is unchanged
        assert df.flags.writeable
        
        # Test that data is the same
        assert frozen_df.equals(df)


class TestValidationUtilities:
    """Test validation utility functions."""
    
    @given(st.integers(min_value=1, max_value=100), st.integers(min_value=1, max_value=10))
    def test_validate_non_empty_dataframe(self, rows: int, cols: int):
        """Test validation of non-empty DataFrames."""
        from src.functional_utils import validate_non_empty_dataframe
        
        # Create non-empty DataFrame
        data = {f'col_{i}': list(range(rows)) for i in range(cols)}
        df = pd.DataFrame(data)
        
        result = validate_non_empty_dataframe(df)
        
        assert result.success is True
        assert result.data.equals(df)
    
    def test_validate_empty_dataframe(self):
        """Test validation of empty DataFrame."""
        from src.functional_utils import validate_non_empty_dataframe
        
        empty_df = pd.DataFrame()
        result = validate_non_empty_dataframe(empty_df)
        
        assert result.success is False
        assert "empty" in result.error.lower()
    
    @given(st.lists(st.text(min_size=1), min_size=1, max_size=10, unique=True))
    def test_validate_required_columns_success(self, columns: list[str]):
        """Test validation when all required columns are present."""
        from src.functional_utils import validate_required_columns
        
        # Create DataFrame with required columns
        data = {col: [1, 2, 3] for col in columns}
        df = pd.DataFrame(data)
        
        result = validate_required_columns(df, columns)
        
        assert result.success is True
        assert result.data.equals(df)
    
    @given(st.lists(st.text(min_size=1), min_size=1, max_size=5, unique=True),
           st.lists(st.text(min_size=1), min_size=1, max_size=5, unique=True))
    def test_validate_required_columns_missing(self, present_cols: list[str], required_cols: list[str]):
        """Test validation when some required columns are missing."""
        from src.functional_utils import validate_required_columns
        
        assume(not set(required_cols).issubset(set(present_cols)))
        
        # Create DataFrame with only present columns
        data = {col: [1, 2, 3] for col in present_cols}
        df = pd.DataFrame(data)
        
        result = validate_required_columns(df, required_cols)
        
        assert result.success is False
        assert "missing" in result.error.lower()


class TestConfigurationTypes:
    """Test immutable configuration types."""
    
    @given(extraction_config_strategy())
    def test_extraction_config_immutability(self, config: ExtractionConfig):
        """Test that ExtractionConfig is immutable."""
        # Test that we can't modify attributes
        with pytest.raises(AttributeError):
            config.dataset_name = "new_name"  # type: ignore
        
        with pytest.raises(AttributeError):
            config.year = 2025  # type: ignore
    
    @given(query_params_strategy())
    def test_query_params_immutability(self, params: QueryParams):
        """Test that QueryParams is immutable."""
        # Test that we can't modify attributes
        with pytest.raises(AttributeError):
            params.model_name = "new_model"  # type: ignore
        
        with pytest.raises(AttributeError):
            params.limit = 100  # type: ignore


class TestPropertyBasedIntegration:
    """Integration tests using property-based testing."""
    
    @given(st.lists(st.integers(), min_size=1, max_size=10))
    def test_pipeline_composition_property(self, numbers: list[int]):
        """Test that pipeline composition is associative."""
        from src.functional_utils import build_pipeline
        
        # Define simple transformation functions
        add_one = lambda x: CLIResult.ok(x + 1)
        multiply_two = lambda x: CLIResult.ok(x * 2)  
        subtract_three = lambda x: CLIResult.ok(x - 3)
        
        # Test associativity: (f ∘ g) ∘ h = f ∘ (g ∘ h)
        pipeline1 = build_pipeline(add_one, multiply_two, subtract_three)
        pipeline2 = build_pipeline(add_one, build_pipeline(multiply_two, subtract_three))
        
        for num in numbers:
            result1 = pipeline1(num)
            result2 = pipeline2(num)
            
            if result1.success and result2.success:
                assert result1.data == result2.data
    
    @given(st.text(), st.integers(min_value=0, max_value=5))
    def test_result_chain_identity_law(self, data: str, chain_length: int):
        """Test that chaining with identity preserves the result (monad left identity)."""
        identity = lambda x: CLIResult.ok(x)
        
        result = CLIResult.ok(data)
        
        # Chain with identity multiple times
        for _ in range(chain_length):
            result = result.flat_map(identity)
        
        assert result.success is True
        assert result.data == data
    
    @given(st.text(min_size=1))
    def test_error_propagation_property(self, error_msg: str):
        """Test that errors propagate through chains without change."""
        identity = lambda x: CLIResult.ok(x)
        
        result = CLIResult.error(error_msg)
        
        # Chain multiple operations
        chained = (result
                  .flat_map(identity)
                  .map(lambda x: x.upper())
                  .flat_map(identity))
        
        assert chained.success is False
        assert chained.error == error_msg