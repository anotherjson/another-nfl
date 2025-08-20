"""
Used to extract data from nfl_data_py
"""

import logging
from typing import Callable
from functools import partial
import click
from another_nfl.nfl_data import extract
from another_nfl import config
from another_nfl.utility import setup_logging


logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    help="Set the logging level.",
)
def cli(log_level: str) -> None:
    """
    A CLI tool for working with nflverse data.

    Arg:
        log_level (str): Sets the logging level for the cli tooling.

    Return:
        None
    """
    setup_logging(level=log_level)


@cli.command()
@click.option(
    "--source",
    type=click.Choice(config.AVAILABLE_SOURCES),
    default="weekly",
    help="Pick a source from the available sources.",
)
@click.option(
    "--start-year",
    type=int,
    help="The first year that is included in the pull. Default is first year available.",
)
@click.option(
    "--end-year",
    type=int,
    required=True,
    help="The last year that is included in the pull.",
)
def load_source(source: str, start_year: int, end_year: int) -> None:
    click.secho(f"Starting data load for source: '{source}'...", fg="cyan")
    logger.info(
        f"CLI parameters received: source={source}, start_year={start_year}, end_year={end_year}"
    )
    try:
        source_config = config.make_config_available(source)
        logger.debug(f"Found configuration for source '{source}'.")

        build_url_partial = partial(
            extract.build_url,
            prefix=config.NFLVERSE_GLOBAL_CONFIG.url_prefix,
            source_prefix=source_config.source_prefix,
            file_type=source_config.file_type,
        )

        query_db_partial = partial(
            extract.query_db, database_path=config.NFLVERSE_GLOBAL_CONFIG.database_path
        )
        logger.debug("Partial functions for URL building and DB querying created.")

        model_start_year = start_year or source_config.start_year
        model_end_year = end_year + 1
        if model_start_year >= model_end_year:
            raise ValueError(f"{start_year} is greater than {end_year}")

        years = extract.create_years(model_start_year, end_year)
        logger.info(
            f"Generated {len(years)} years to process: from {model_start_year} to {end_year}"
        )

        urls = extract.build_url_list(year_list=years, build_url_func=build_url_partial)
        logger.info(f"Generated {len(urls)} URLs for ingestion.")
        logger.debug(f"First URL: {urls[0] if urls else 'N/A'}")

        query = extract.create_table(source, urls)
        logger.debug("Generated SQL CREATE TABLE query.")

        click.echo("Executing database query...")
        query_db_partial(query)

        click.secho(f"Data for source ' {source}' loaded.", fg="green")

    except Exception as e:
        logger.exception("Critical error occurred during load_source:")
        click.secho(f"Error: {e}", fg="red")


@cli.command()
@click.option("--query", type=str, default="select 1", help="Query the database.")
def query_table(query: str) -> None:
    click.echo(f"Running query: {query}")
    logger.info(f"Executing user-provided query: {query}")
    try:
        query_db_partial = partial(
            extract.query_db, database_path=config.NFLVERSE_GLOBAL_CONFIG.database_path
        )
        query_db_partial(sql_query=query, show_table=True)

    except Exception as e:
        logger.exception("A critical error occurred during query_table:")
        click.secho(f"Error: {e}", fg="red")
