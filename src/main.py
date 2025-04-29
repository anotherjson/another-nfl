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

    pick_table = args.pick_table
    exclude_table = args.exclude_table
    schema_name = args.schema
    schema_staging = f"{schema_name}_staging"

    years_beg = utility.convert_to_int(args.years_beg)
    years_end = utility.convert_to_int(args.years_end)
    years = list(range(years_beg, years_end))
    logger.debug("Years are from: %s, %s, becomes %s", years_beg, years_end, years)

    # Setting up variables and functions for reuse in loop
    engine = utility.make_engine(url, utility.test_db_engine)

    partial_write_table = functools.partial(
        db.write_to_table,
        connection_engine=engine,
        exist_behavour="replace",
    )

    partial_find_new = functools.partial(
        db.find_new_columns,
        connection_engine=engine,
        name_schema_existing=schema_name,
        name_schema_staging=schema_staging,
    )

    partial_merge_staging = functools.partial(
        db.merge_staging,
        connection_engine=engine,
        name_schema_existing=schema_name,
        name_schema_staging=schema_staging,
    )

    partial_find_new = functools.partial(
        db.find_new_columns,
        connection_engine=engine,
        name_schema_existing=schema_name,
        name_schema_staging=schema_staging,
    )

    partial_alter_schema = functools.partial(
        db.alter_schema,
        connection_engine=engine,
        name_schema=schema_name,
    )

    for data_key, config in nfl_mapping.DATA_TABLE_MAP.items():
        print(f"{data_key}")
        if pick_table not in ("all", data_key):
            logger.debug("Skipping table: %s", data_key)
            print(f"Skipping table: {data_key}")
            continue

        if exclude_table is not None and data_key in (exclude_table):
            logger.debug("Skipping table: %s", data_key)
            print(f"Skipping table: {data_key}")
            continue

        logger.info("Starting: %s", data_key)
        table_name = config["table_name"]

        partial_func_nfl = nfl_mapping.build_nfl_function(config)

        if config["year_accepted"]:
            for year in years:
                logger.info("Starting year: %s", year)
                print(f"Starting {year}")

                partial_func_nfl = functools.partial(partial_func_nfl, years=[year])

                inspector = inspect(engine)
                logger.debug("inspector is: %s", inspector)

                partial_find_table = functools.partial(
                    db.find_table_columns, inspector=inspector
                )

                if_exists = inspector.has_table(
                    table_name=table_name, schema=schema_name
                )
                logger.info("Table status: %s", if_exists)

                if if_exists:
                    db.write_imported(
                        import_func=partial_import_data,
                        nfl_func=partial_func_nfl,
                        write_func=partial_write_table,
                        name_table=table_name,
                        name_schema=schema_staging,
                    )

                    db.insert_from_staging(
                        func_find_new=partial_find_new,
                        func_find_table=partial_find_table,
                        func_alter_schema=partial_alter_schema,
                        func_merge_staging=partial_merge_staging,
                        name_table=table_name,
                    )

                else:
                    db.write_imported(
                        import_func=partial_import_data,
                        nfl_func=partial_func_nfl,
                        write_func=partial_write_table,
                        name_table=table_name,
                        name_schema=schema_name,
                    )
        else:
            db.write_imported(
                import_func=partial_import_data,
                nfl_func=partial_func_nfl,
                write_func=partial_write_table,
                name_table=table_name,
                name_schema=schema_name,
            )


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
        default=1999,
        help="First year to pull player data",
    )
    parser.add_argument(
        "--years_end",
        default=2025,
        help="Ending year to pull player data",
    )
    parser.add_argument(
        "--schema",
        default="lake",
        help="Schema name for table",
    )
    parser.add_argument(
        "--pick_table",
        default="all",
        help="Allows updating only one table",
    )
    parser.add_argument(
        "--exclude_table",
        default=None,
        help="Allows skipping updating a table",
    )
    args = parser.parse_args()

    main(args)
