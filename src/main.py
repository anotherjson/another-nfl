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

    # Setting up variables that are constaint for reuse
    engine, inspector = utility.make_engine_inspector(url, utility.test_db_engine)

    schema_name = args.schema
    schema_staging = f"{schema_name}_staging"
    years = list(range(args.years_beg, args.years_end))

    for data_key, config in nfl_mapping.DATA_TABLE_MAP.items():
        if data_key == "pbp_data":
            table_name = config["table_name"]

            if isinstance(config["downcast_type"], bool):
                partial_func = functools.partial(
                    config["fetch_func"], downcast=config["downcast_type"]
                )
            else:
                partial_func = config["fetch_func"]

            if config["year_accepted"]:
                for year in years:
                    logger.info("Starting year: %s", year)
                    print(f"Starting {year}")

                    if db.does_table_exist(
                        inspector=inspector,
                        table_name=table_name,
                        schema_name=schema_name,
                    ):
                        existing_columns = db.find_table_columns(
                            inspector=inspector,
                            table_name=table_name,
                            schema_name=schema_name,
                        )
                        imported_data = partial_import_data(
                            nfl_function=partial_func,
                            year=year,
                        )
                        imported_data.partial_to_sql(
                            name=config["table_name"],
                            con=engine,
                            schema=schema_staging,
                        )

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
