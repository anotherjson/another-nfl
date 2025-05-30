"""[TODO:write docstring]"""

import logging
import pathlib
import os
from collections.abc import Callable

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine, Inspector

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
    logger.debug("Log file path is: %s", file_path)


def create_db_url(host: str, port: str, db: str, user: str, pwd: str, ssl: str):
    """[TODO:write docstring]"""
    # Create db connection
    db_host = os.getenv(host)
    db_port = os.getenv(port)
    db_db = os.getenv(db)
    db_user = os.getenv(user)
    db_pwd = os.getenv(pwd)
    db_ssl = os.getenv(ssl)

    # Validate that variables are loaded
    if not all([host, port, db, user, pwd, ssl]):
        raise ValueError("One r more database environment variables are not set.")

    # Create url
    db_url = f"postgresql+psycopg2://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_db}?sslmode={db_ssl}"
    db_url_log = f"postgresql+psycopg2://{db_user}:***@{db_host}:{db_port}/{db_db}?sslmode={db_ssl}"
    logger.debug("Created db connection url as: %s", db_url_log)

    return db_url


def test_db_engine(engine: Engine) -> None:
    """[TODO:write docstring]"""
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            db_version = result.scalar()
            logger.debug(
                "Successfully connected to PostgreSQL! Version: %s", db_version
            )

        return True

    except Exception as e:
        logger.error("Error connecting to the database: %s", e)
        return False


def make_engine(url: str, testing_func: Callable) -> Engine:
    """[TODO:write docstring]"""
    engine = create_engine(url, pool_pre_ping=True)
    logger.debug("Engine is: %s", engine)
    engine_working = testing_func(engine)
    logger.debug("Engine tested: %s", engine_working)

    return engine


def convert_to_int(arg_field: [str, int]) -> int:
    """[TODO:write docstring]"""
    if isinstance(arg_field, str):
        return int(arg_field)

    return arg_field
