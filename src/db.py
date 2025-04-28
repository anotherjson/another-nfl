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


def does_table_exists(inspector: Inspector, name_table: str, name_schema: str) -> bool:
    """[TODO:description]"""
    if_exists = inspector.has_table(table_name=name_table, schema=name_schema)
    if if_exists:
        logging.debug("Table missing: %s.%s", name_schema, name_table)

    return if_exists


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
    name_table_existing: str,
    name_table_staging: str,
    name_schema_existing: str,
    name_schema_staging: str,
) -> dict:
    """[TODO:description]"""
    existing_columns = func_find_table(
        name_table=name_table_existing, name_schema=name_schema_existing
    )
    staging_columns = func_find_table(
        name_table=name_table_staging, name_schema=name_schema_staging
    )
    new_columns = set(staging_columns.keys()) - set(existing_columns.keys())
    new_column_types = {col: staging_columns[col] for col in new_columns}

    logger.debug("New columns found: %s", new_column_types)

    return new_column_types


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
                col_type = new_columns_types["col"]
                alter_sql = text(
                    f'ALTER TABLE "{name_schema}"."{name_table}" ADD COLUMN {quoted_col} {col_type} NULL;'
                )
                logging.debug("Alter sql is: %s", alter_sql)
                connection.execute(alter_sql)
