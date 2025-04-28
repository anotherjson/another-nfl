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


def does_table_exist(inspector: Inspector, table_name: str, schema_name: str) -> bool:
    """[TODO:description]"""
    if_exists = inspector.has_table(table_name=table_name, schema=schema_name)
    if if_exists:
        logging.info("Table missing: %s.%s", schema_name, table_name)

    return if_exists


def find_table_columns(
    inspector: Inspector,
    table_name: str,
    schema_name: str,
) -> dict:
    """[TODO:description]"""
    existing_columns_info = inspector.get_columns(table_name, schema=schema_name)
    existing_columns = {col["name"] for col in existing_columns_info}
    logger.info("Existing columns: %s", existing_columns)

    return existing_columns


def import_data(
    nfl_function: Callable,
    date_loaded: datetime,
    year: [int, None] = None,
) -> pd.DataFrame:
    """[TODO:description]"""
    if isinstance(year, int):
        df = nfl_function([year])
    else:
        print("issue with year")
        df = nfl_function()

    df = df.copy()
    df["_date_loaded"] = date_loaded

    return df


def write_to_table(
    nfl_func: Callable,
    import_func: Callable,
    write_func: Callable,
    table_name: str,
    engine_info: Engine,
    schema_name: str,
) -> None:
    """[TODO:description]"""
    df.to_sql(
        name=table_name,
        con=engine_info,
        schema=schema_name,
        if_exists=exists_type,
        index=False,
        chunksize=1000,
    )
