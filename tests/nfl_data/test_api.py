import pandas as pd
import pytest
from requests.exceptions import RequestException

from another_nfl.nfl_data import api


@pytest.fixture
def sample_pbp_data():
    """Provides a sample DataFrame for mocking the API call."""
    return pd.DataFrame(
        {
            "player_id": ["00-001", "00-002", "00-003"],
            "season": [2023, 2023, 2023],
            "week": [1, 1, 2],
            "team": ["KC", "PHI", "KC"],
            "receptions": [5, 2, 7],
        }
    )


def test_execute_call_success(sample_pbp_data):
    """
    Tests that _execute_call correctly returns the data from a successful
    function call.
    """

    # Arrange: 1. Define a simple, fake function that simulates a successful call.
    def fake_successful_func(years=None):
        return sample_pbp_data

    # Act: 2. Call _execute_call with our fake function.
    result = api._execute_call(fake_successful_func, years=[2023])

    # Assert: 3. Check that it returned the data from our fake function.
    assert result is not None
    assert result.equals(sample_pbp_data)


def test_execute_call_handles_exception(caplog):
    """
    Tests that _execute_call returns None and logs an error when the
    wrapped function raises an exception.
    """
    # Arrange: 1. Define a fake function that will always fail.
    error_message = "This is a test error"

    def fake_failing_func(years=None):
        raise ValueError(error_message)

    # Act: 2. Call _execute_call with our failing function.
    result = api._execute_call(fake_failing_func, years=[2023])

    # Assert: 3. Check that the result is None and the error was logged.
    assert result is None
    assert "API call to" in caplog.text
    assert "failed" in caplog.text
    assert error_message in caplog.text


def test_get_pbp_data_success(mocker, sample_pbp_data):
    """
    Tests the successful retrieval of pbp data.
    """
    # Arrange: Mock the external dependency
    mock_import = mocker.patch(
        "nfl_data_py.import_pbp_data", return_value=sample_pbp_data
    )

    # Act: Call the function we are testing
    result_df = api.get_pbp_data(years=[2023])

    # Assert: Check the outcome
    mock_import.assert_called_once_with(years=[2023])
    assert result_df is not None
    assert len(result_df) == 3

