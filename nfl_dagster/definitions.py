"""Dagster definitions for the NFL analytics pipeline with complete Dagster-managed dbt integration."""

from dagster import Definitions, load_assets_from_modules

from nfl_dagster import assets
from nfl_dagster.resources import duckdb_resource, dbt_resource
from nfl_dagster.resources.ducklake_resource import ducklake_resource
from nfl_dagster.schedules.nfl_data_schedules import (
    daily_critical_data_extraction,
    weekly_high_priority_extraction,
    weekly_medium_priority_extraction,
    daily_critical_staging_processing,
    weekly_high_priority_staging_processing,
    daily_intermediate_processing,
    daily_data_health_check,
    weekly_staging_validation,
    nfl_season_intensive_processing,
    monthly_full_pipeline_refresh,
)
from nfl_dagster.jobs.nfl_pipeline_jobs import (
    nfl_critical_raw_data_job,
    nfl_high_priority_raw_data_job,
    nfl_medium_priority_raw_data_job,
    nfl_low_priority_raw_data_job,
    nfl_critical_staging_job,
    nfl_high_priority_staging_job,
    nfl_medium_priority_staging_job,
    nfl_intermediate_models_job,
    nfl_data_health_check_job,
    nfl_staging_validation_job,
    nfl_full_critical_pipeline_job,
    nfl_seasonal_intensive_job,
    nfl_full_pipeline_refresh_job,
)


# Load all assets
all_assets = load_assets_from_modules([assets])

# Define the Dagster definitions
defs = Definitions(
    assets=all_assets,
    schedules=[
        # Daily schedules
        daily_critical_data_extraction,
        daily_critical_staging_processing,
        daily_intermediate_processing,
        daily_data_health_check,
        
        # Weekly schedules
        weekly_high_priority_extraction,
        weekly_medium_priority_extraction,
        weekly_high_priority_staging_processing,
        weekly_staging_validation,
        
        # Seasonal/conditional schedules
        nfl_season_intensive_processing,
        monthly_full_pipeline_refresh,
    ],
    jobs=[
        # Raw data extraction jobs
        nfl_critical_raw_data_job,
        nfl_high_priority_raw_data_job,
        nfl_medium_priority_raw_data_job,
        nfl_low_priority_raw_data_job,
        
        # Staging processing jobs
        nfl_critical_staging_job,
        nfl_high_priority_staging_job,
        nfl_medium_priority_staging_job,
        
        # Intermediate and monitoring jobs
        nfl_intermediate_models_job,
        nfl_data_health_check_job,
        nfl_staging_validation_job,
        
        # Comprehensive pipeline jobs
        nfl_full_critical_pipeline_job,
        nfl_seasonal_intensive_job,
        nfl_full_pipeline_refresh_job,
    ],
    resources={
        "duckdb": duckdb_resource,
        "dbt": dbt_resource,
        "ducklake": ducklake_resource,
    },
)