import logging
import pandas as pd
import nfl_data_py as nfl
from typing import Optional, Callable, Any
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


def _execute_call(nfl_function: Callable, **kwargs: Any) -> Optional[pd.DataFrame]:
    """
    A simple, generic helper to execute an nfl_data_py function
    and handle any resulting exceptions.
    """
    try:
        return nfl_function(**kwargs)
    except Exception as e:
        logger.error(f"API call to '{nfl_function}' failed: {e}")
        return None


def get_pbp_data(years: list[int]) -> Optional[pd.DataFrame]:
    """
    Pulls from play by play data for years in a list.
    Earliest year is 1999.
    A year list too long will timeout.

    Args:
        years (list[int]): A list of years as YYYY int

    Return:
        Pandas dataframe of all columns in the pbp dataset
        for years stated.
    """
    return _execute_call(nfl.import_pbp_data, years=years)


def get_weekly_data(years: list[int]) -> Optional[pd.DataFrame]:
    """
    Pulls from weekly data for years in a list.
    Earliest year is 1999.
    A year list too long will timeout.

    Args:
        years (list[int]): A list of years as YYYY int.

    Return:
        Pandas dataframe of all columns in the weekly dataset
        for years stated.
    """
    return _execute_call(nfl.import_weekly_data, years=years)


def get_seasonal_data(
    years: list[int], season_type: str = "ALL"
) -> Optional[pd.DataFrame]:
    """
    Pulls from season data for years in a list.
    Earliest year is 1999.
    A year list too long will timeout.

    Args:
        years (list[int]): A list of years as YYYY int.
        season_type (str): A string of either ALL, REG, POST. Defaults to ALL.

    Return:
        Pandas dataframe of all columns in the seasonal dataset
        for years stated.
    """
    return _execute_call(
        nfl.import_seasonal_data,
        years=years,
        s_type=season_type,
    )


def get_seasonal_rosters(
    years: list[int],
) -> Optional[pd.DataFrame]:
    """
    Pulls from season roster data for years in a list.
    Earliest year is 1999.
    A year list too long will timeout.

    Args:
        years (list[int]): A list of years as YYYY int.

    Return:
        Pandas dataframe of all columns in the seasonal roster dataset
        for years stated.
    """
    return _execute_call(
        nfl.import_seasonal_data,
        years=years,
    )


def get_weekly_rosters(
    years: list[int],
) -> Optional[pd.DataFrame]:
    """
    Pulls from weekly roster data for years in a list.
    Earliest year is 1999.
    A year list too long will timeout.

    Args:
        years (list[int]): A list of years as YYYY int.

    Return:
        Pandas dataframe of all columns in the weekly roster dataset
        for years stated.
    """
    return _execute_call(
        nfl.import_weekly_rosters,
        years=years,
    )


def get_win_totals(
    years: list[int],
) -> Optional[pd.DataFrame]:
    """
    Pulls from win totals data for years in a list.
    Earliest year is unknown.
    A year list too long will timeout.

    Args:
        years (list[int]): A list of years as YYYY int.

    Return:
        Pandas dataframe of all columns in the weekly roster dataset
        for years stated.
    """
    return _execute_call(
        nfl.import_win_totals,
        years=years,
    )


def main():
    print(get_seasonal_rosters(years=[2023]))


if __name__ == "__main__":
    main()
