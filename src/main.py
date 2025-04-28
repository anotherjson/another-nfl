"""[TODO:description]"""

# Python packages
import logging
import pathlib
import os
import argparse
import functools
from datetime import datetime, timezone
from collections.abc import Callable

# 3rd party packages
import pandas as pd
import nfl_data_py as nfl
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.engine import Engine

# Import src modules
import utility
import db
import nfl_mapping

# Setup logger for script
logger = logging.getLogger(__name__)


def main(args: argparse.Namespace) -> None:
    cwd = pathlib.Path.cwd()

    # Confirm log file exists, if not make one
    utility.create_log_file(
        dir_current=cwd, dir_name=args.logger_dir, file_name=args.logger_filename
    )

    # ETL date to add when loading files
    now = datetime.now(tz=timezone.utc)
    partial_import_data = functools.partial(db.import_data, date_loaded=now)

    # Create db connection
    url = utility.create_db_url(
        host=args.env_host,
        port=args.env_port,
        db=args.env_db,
        user=args.env_user,
        pwd=args.env_pwd,
        ssl=args.env_ssl,
    )

    # Setting up variables and functions for reuse in loop
    engine, inspector = utility.make_engine_inspector(url, utility.test_db_engine)

    schema_name = args.schema
    schema_staging = f"{schema_name}_staging"
    years = list(range(args.years_beg, args.years_end))

    partial_write_table = functools.partial(
        db.write_to_table,
        connection_engine=engine,
        name_schema=schema_staging,
        exist_behavour="replace",
    )

    partial_find_table = functools.partial(db.find_table_columns, inspector=inspector)

    partial_exists_table = functools.partial(
        db.does_table_exists, inspector=inspector, name_schema=schema_name
    )

    partial_find_new = functools.partial(
        db.find_new_columns,
        func_find_table=partial_find_table,
        name_schema_existing=schema_name,
        name_schema_staging=schema_staging,
    )

    for data_key, config in nfl_mapping.DATA_TABLE_MAP.items():
        if data_key == "pbp_data":
            table_name = config["table_name"]

            if isinstance(config["downcast_type"], bool):
                partial_func_nfl = functools.partial(
                    config["fetch_func"], downcast=config["downcast_type"]
                )
            else:
                partial_func_nfl = config["fetch_func"]

            if config["year_accepted"]:
                for year in years:
                    logger.info("Starting year: %s", year)
                    print(f"Starting {year}")

                    partial_func_nfl = functools.partial(partial_func_nfl, years=[year])

                    if partial_exists_table(name_table=table_name):
                        existing_columns_types = partial_find_table(
                            name_table=table_name, name_schema=schema_name
                        )
                        # imported_data = partial_import_data(
                        #     nfl_function=partial_func_nfl,
                        #     year=year,
                        # )
                        # partial_write_table(
                        #     dataframe=imported_data, name_table=table_name
                        # )
                        new_columns = partial_find_new(
                            name_table_existing=table_name,
                            name_table_staging=table_name,
                        )
                        print(new_columns)

                    break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Info to load data from nfl_data_py to db"
    )
    parser.add_argument(
        "--logger_dir",
        type=str,
        default="data",
        help="The directory to save the log file in",
    )
    parser.add_argument(
        "--logger_filename",
        type=str,
        default="debug.log",
        help="What the log file should be named",
    )
    parser.add_argument(
        "--env_host",
        default="AJ_HOST",
        help="Host for connection, set in .profile, .zprofile or local",
    )
    parser.add_argument(
        "--env_port",
        default="AJ_PORT",
        help="Port for connection, set in .profile, .zprofile or local",
    )
    parser.add_argument(
        "--env_db",
        default="AJ_DB_NFL",
        help="DB for connection, set in .profile, .zprofile or local",
    )
    parser.add_argument(
        "--env_user",
        default="AJ_USER",
        help="User for connection, set in .profile, .zprofile or local",
    )
    parser.add_argument(
        "--env_pwd",
        default="AJ_PWD",
        help="Password for connection, set in .profile, .zprofile or local",
    )
    parser.add_argument(
        "--env_ssl",
        default="AJ_SSL",
        help="SSL for connection, set in .profile, .zprofile or local",
    )
    parser.add_argument(
        "--years_beg",
        default=2016,
        help="First year to pull player data",
    )
    parser.add_argument(
        "--years_end",
        default=2024,
        help="Ending year to pull player data",
    )
    parser.add_argument(
        "--schema",
        default="lake",
        help="Schema name for table",
    )
    parser.add_argument(
        "--table_pbp",
        default="pbp",
        help="Table name for play by play data",
    )
    args = parser.parse_args()

    main(args)
