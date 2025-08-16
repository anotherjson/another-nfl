import duckdb
from another_nfl import config
from functools import partial


def create_years(start_year: int, end_year: int) -> list[int]:
    return list(range(start_year, end_year))


def build_url(prefix: str, source_prefix: str, year: int, file_type: str) -> str:
    return f"{prefix}{source_prefix}{year}{file_type}"


def build_url_list(year_list: list[int], build_url_func) -> list[str]:
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


def query_db(sql_query: str, db_name: str):
    with duckdb.connect(db_name) as con:
        con.sql(sql_query)


build_url_partial = partial(
    build_url,
    prefix=config.NFLVERSE_GLOBAL_CONFIG.url_prefix,
    source_prefix=config.NFLVERSE_SOURCES[0].source_prefix,
    file_type=config.NFLVERSE_SOURCES[0].file_type,
)


def run_extraction():
    years = create_years(config.NFLVERSE_SOURCES[0].start_year, 2025)
    urls = build_url_list(year_list=years, build_url_func=build_url_partial)

    query = create_table(config.NFLVERSE_SOURCES[0].source_name, urls)

    query_db(query, config.NFLVERSE_GLOBAL_CONFIG.database_path)

    with duckdb.connect(config.NFLVERSE_GLOBAL_CONFIG.database_path) as con:
        con.sql("select * from weekly;").show()


if __name__ == "__main__":
    run_extraction()
