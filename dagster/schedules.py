"""Dagster schedules for the NFL analytics pipeline."""

from dagster import schedule, RunRequest, ScheduleEvaluationContext, DefaultScheduleStatus
from .assets.raw_data_assets import pbp_data, weekly_data, schedules_data
from .assets.dbt_assets import dbt_staging_models, dbt_intermediate_models, dbt_marts_models


@schedule(
    cron_schedule="0 6 * * 2",  # Every Tuesday at 6 AM
    job_name="weekly_extraction_job",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Weekly extraction of NFL data after games complete"
)
def weekly_extraction_schedule(context: ScheduleEvaluationContext):
    """Schedule for weekly data extraction."""
    return RunRequest(
        asset_selection=[
            pbp_data,
            weekly_data,
            schedules_data,
        ],
        tags={
            "schedule": "weekly_extraction",
            "extraction_type": "incremental"
        }
    )


@schedule(
    cron_schedule="0 8 * * 2",  # Every Tuesday at 8 AM (after extraction)
    job_name="dbt_transformation_job", 
    default_status=DefaultScheduleStatus.STOPPED,
    description="dbt transformations after data extraction"
)
def dbt_transformation_schedule(context: ScheduleEvaluationContext):
    """Schedule for dbt model runs."""
    return RunRequest(
        asset_selection=[
            dbt_staging_models,
            dbt_intermediate_models,
            dbt_marts_models,
        ],
        tags={
            "schedule": "dbt_transformation",
            "transformation_type": "full_refresh"
        }
    )


@schedule(
    cron_schedule="0 4 1 * *",  # First day of every month at 4 AM
    job_name="monthly_full_refresh_job",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Monthly full refresh of all data and models"
)
def monthly_full_refresh_schedule(context: ScheduleEvaluationContext):
    """Schedule for monthly full data refresh."""
    return RunRequest(
        asset_selection="*",  # All assets
        tags={
            "schedule": "monthly_full_refresh",
            "refresh_type": "complete"
        }
    )