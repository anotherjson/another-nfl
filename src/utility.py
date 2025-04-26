"""[TODO:write docstring]"""

import logging
import pathlib

logger = logging.getLogger(__name__)


def create_log_file(
    dir_current: pathlib.PosixPath,
    dir_name: str,
    file_name: str,
    encoding_type: str = "utf-8",
) -> None:
    """[TODO:write docstring]"""
    dir_path = dir_current / dir_name
    file_path = dir_path / file_name

    dir_path.mkdir(exist_ok=True)
    logging.basicConfig(filename=file_path, encoding=encoding_type, level=logging.DEBUG)
    logger.info("Log file path is: %s", file_path)
