"""[TODO:description]"""

# Python packages
import logging
import pathlib
import os
import argparse
import functools
from datetime import datetime
from collections.abc import Callable

# 3rd party packages
import nfl_data_py as nfl

# Import src modules
import utility

# Setup logger for script
logger = logging.getLogger(__name__)


def main(args: argparse.Namespace) -> None:
    cwd = pathlib.Path.cwd()

    # Confirm log file exists, if not make one
    utility.create_log_file(
        dir_current=cwd, dir_name=args.logger_dir, file_name=args.logger_filename
    )

    # ETL date to add when loading files
    now = datetime.now()
    now_suffix = now.strftime("%Y%m%d%H%M%S")
    logger.info("Suffix for _date_loaded: %s", now_suffix)


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
    args = parser.parse_args()

    main(args)
