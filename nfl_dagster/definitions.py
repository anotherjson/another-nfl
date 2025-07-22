"""Dagster definitions for the NFL analytics pipeline."""

from dagster import Definitions, load_assets_from_modules

from nfl_dagster import assets
from nfl_dagster.resources import duckdb_resource, dbt_resource
# from nfl_dagster.schedules import weekly_extraction_schedule, dbt_transformation_schedule


# Load all assets
all_assets = load_assets_from_modules([assets])

# Define the Dagster definitions
defs = Definitions(
    assets=all_assets,
    # schedules=[
    #     weekly_extraction_schedule,
    #     dbt_transformation_schedule,
    # ],
    resources={
        "duckdb": duckdb_resource,
        "dbt": dbt_resource,
    },
)