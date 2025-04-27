import logging
import argparse
import functools
from datetime import datetime
from collections.abc import Callable

# 3rd party packages
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Setup logger for script
logger = logging.getLogger(__name__)


def append_to_table(
    nfl_function: Callable,
    engine_info: Engine,
    date_loaded: datetime,
    table_name: str,
    schema_name: str,
    column_rename: [dict, None] = None,
    year: [str, None] = None,
) -> None:
    if isinstance(year, str):
        df = nfl_function(year)
    else:
        df = nfl_function()

    if isinstance(column_rename, dict):
        df = df.rename(column_rename, axis=1)

    df = df.copy()
    df["_date_loaded"] = date_loaded
    df.to_sql(
        name=table_name,
        con=engine_info,
        schema=schema_name,
        if_exists="append",
        index=False,
        chunksize=1000,
    )
