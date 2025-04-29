import logging
import argparse
import functools
from datetime import datetime
from collections.abc import Callable

# 3rd party packages
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.engine import Engine, Inspector

# Setup logger for script
logger = logging.getLogger(__name__)


def find_table_columns(
    inspector: Inspector,
    name_table: str,
    name_schema: str,
) -> dict:
    """[TODO:description]"""
    table_columns_info = inspector.get_columns(
        table_name=name_table, schema=name_schema
    )
    table_columns = {col["name"]: col["type"] for col in table_columns_info}
    logger.debug("Table columns are: %s", table_columns)

    return table_columns


def import_data(
    nfl_function: Callable,
    date_loaded: datetime,
    year: [int, None] = None,
) -> pd.DataFrame:
    """[TODO:description]"""
    logger.debug(
        "Using nfl function for year with date: %s, %s, %s",
        nfl_function,
        date_loaded,
        year,
    )
    df = nfl_function()
    df = df.copy()
    df["_date_loaded"] = date_loaded

    return df


def write_to_table(
    dataframe: pd.DataFrame,
    name_table: str,
    connection_engine: Engine,
    name_schema: str,
    exist_behavour: str,
) -> None:
    """[TODO:description]"""
    logger.debug("Building table on schema and table: %s.%s", name_schema, name_table)
    dataframe.to_sql(
        name=name_table,
        con=connection_engine,
        schema=name_schema,
        if_exists=exist_behavour,
        index=False,
        chunksize=1000,
    )


def find_new_columns(
    func_find_table: Callable,
    connection_engine: Engine,
    name_table: str,
    name_schema_existing: str,
    name_schema_staging: str,
) -> dict:
    """[TODO:description]"""
    existing_columns = func_find_table(
        name_table=name_table, name_schema=name_schema_existing
    )
    staging_columns = func_find_table(
        name_table=name_table, name_schema=name_schema_staging
    )
    new_columns = set(staging_columns.keys()) - set(existing_columns.keys())
    new_column_types = {
        col: staging_columns[col].compile(dialect=connection_engine.dialect)
        for col in new_columns
    }

    logger.debug("New columns found: %s", new_column_types)

    return staging_columns, new_column_types


def alter_schema(
    new_columns_types: dict,
    name_table: str,
    name_schema: str,
    connection_engine: Engine,
) -> None:
    """[TODO:description]"""
    with connection_engine.connect() as connection:
        with connection.begin():
            logging.debug("Connection is: %s", connection)
            for col in new_columns_types:
                quoted_col = f'"{col}"'
                col_type = new_columns_types[col]

                alter_sql = text(
                    f"""
                    ALTER TABLE "{name_schema}"."{name_table}"
                    ADD COLUMN {quoted_col} {col_type} NULL;
                """
                )
                logging.debug("Alter sql is: %s", alter_sql)
                connection.execute(alter_sql)


def merge_staging(
    connection_engine: Engine,
    name_table: str,
    name_schema_existing: str,
    name_schema_staging: str,
    columns_staging: dict,
) -> None:
    """[TODO:description]"""
    with connection_engine.connect() as connection:
        with connection.begin():
            logging.debug("Connection is: %s", connection)
            qouted_columns_staging = [f'"{col}"' for col in columns_staging]
            insert_sql = text(
                f"""
                INSERT INTO "{name_schema_existing}"."{name_table}"
                ({", ".join(qouted_columns_staging)})
                SELECT {", ".join(qouted_columns_staging)}
                FROM "{name_schema_staging}"."{name_table}";
            """
            )
            logging.debug("Insert sql is: %s", insert_sql)
            connection.execute(insert_sql)


def write_imported(
    import_func: Callable,
    nfl_func: Callable,
    write_func: Callable,
    name_table: str,
    name_schema: str,
) -> None:
    """[TODO:description]"""
    imported_data = import_func(nfl_function=nfl_func)

    write_func(dataframe=imported_data, name_table=name_table, name_schema=name_schema)


def insert_from_staging(
    func_find_new: Callable,
    func_find_table: Callable,
    func_alter_schema: Callable,
    func_merge_staging: Callable,
    name_table: str,
) -> None:
    """[TODO:description]"""
    staging_columns, new_columns = func_find_new(
        func_find_table=func_find_table,
        name_table=name_table,
    )

    if not len(new_columns) == 0:
        func_alter_schema(
            new_columns_types=new_columns,
            name_table=name_table,
        )

    func_merge_staging(
        name_table=name_table,
        columns_staging=staging_columns,
    )
