"""
[TODO]make docstring
"""

from typing import Optional, Callable
import duckdb
from another_nfl import config


def create_years(start_year: int, end_year: int) -> list[int]:
    return list(range(start_year, end_year))


def build_url(prefix: str, source_prefix: str, year: int, file_type: str) -> str:
    return f"{prefix}{source_prefix}{year}{file_type}"


def build_url_list(
    year_list: list[int], build_url_func: Callable[[int], str]
) -> list[str]:
    url_list = []
    for year in year_list:
        url = build_url_func(year=year)
        url_list.append(url)

    return url_list


def create_table(table_name: str, url: str | list[str]) -> str:
    sql_built = f"""
    create or replace table {table_name} as
    select
        *,
        now() as _extracted_date
    from
        read_parquet({url}, union_by_name = true, filename = true);
    """
    return sql_built


def query_db(sql_query: str, database_path: str, show_table: bool = False) -> None:
    with duckdb.connect(database_path) as con:
        if show_table:
            con.sql(sql_query).show()
        else:
            con.sql(sql_query)
