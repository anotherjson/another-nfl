"""
Dagster schedules for NFL data pipeline orchestration.

This module defines automated schedules for the complete NFL data pipeline:
- Raw data extraction
- Staging model processing  
- Intermediate model execution
- Data quality validation
"""

from dagster import (
    DefaultScheduleStatus,
    ScheduleDefinition,
    schedule,
    RunRequest,
    SkipReason,
    ScheduleEvaluationContext,
)
from datetime import datetime


# Raw Data Extraction Schedules
@schedule(
    cron_schedule="0 6 * * *",  # Daily at 6 AM
    job_name="nfl_critical_raw_data_job",
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_critical_data_extraction(context: ScheduleEvaluationContext):
    """Schedule daily extraction of critical NFL datasets (pbp, weekly, schedules, team_desc)."""
    return RunRequest(
        run_key=f"critical_extraction_{context.scheduled_execution_time.strftime('%Y_%m_%d')}",
        tags={
            "schedule": "daily_critical_extraction",
            "priority": "critical",
            "data_type": "raw"
        }
    )


@schedule(
    cron_schedule="0 8 * * 1",  # Weekly on Monday at 8 AM
    job_name="nfl_high_priority_raw_data_job",
    default_status=DefaultScheduleStatus.RUNNING,
)
def weekly_high_priority_extraction(context: ScheduleEvaluationContext):
    """Schedule weekly extraction of high priority NFL datasets."""
    return RunRequest(
        run_key=f"high_priority_extraction_{context.scheduled_execution_time.strftime('%Y_%m_%d')}",
        tags={
            "schedule": "weekly_high_priority_extraction",
            "priority": "high",
            "data_type": "raw"
        }
    )


@schedule(
    cron_schedule="0 10 * * 6",  # Weekly on Saturday at 10 AM
    job_name="nfl_medium_priority_raw_data_job",
    default_status=DefaultScheduleStatus.STOPPED,  # Start stopped, enable as needed
)
def weekly_medium_priority_extraction(context: ScheduleEvaluationContext):
    """Schedule weekly extraction of medium priority NFL datasets."""
    return RunRequest(
        run_key=f"medium_priority_extraction_{context.scheduled_execution_time.strftime('%Y_%m_%d')}",
        tags={
            "schedule": "weekly_medium_priority_extraction",
            "priority": "medium",
            "data_type": "raw"
        }
    )


# Staging Model Processing Schedules
@schedule(
    cron_schedule="0 7 * * *",  # Daily at 7 AM (after critical data extraction)
    job_name="nfl_critical_staging_job",
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_critical_staging_processing(context: ScheduleEvaluationContext):
    """Schedule daily processing of critical staging models."""
    return RunRequest(
        run_key=f"critical_staging_{context.scheduled_execution_time.strftime('%Y_%m_%d')}",
        tags={
            "schedule": "daily_critical_staging",
            "priority": "critical",
            "data_type": "staging"
        }
    )


@schedule(
    cron_schedule="0 9 * * 1",  # Weekly on Monday at 9 AM (after high priority extraction)
    job_name="nfl_high_priority_staging_job",
    default_status=DefaultScheduleStatus.RUNNING,
)
def weekly_high_priority_staging_processing(context: ScheduleEvaluationContext):
    """Schedule weekly processing of high priority staging models."""
    return RunRequest(
        run_key=f"high_priority_staging_{context.scheduled_execution_time.strftime('%Y_%m_%d')}",
        tags={
            "schedule": "weekly_high_priority_staging",
            "priority": "high",
            "data_type": "staging"
        }
    )


# Intermediate Model Processing Schedule
@schedule(
    cron_schedule="0 8 * * *",  # Daily at 8 AM (after staging models)
    job_name="nfl_intermediate_models_job",
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_intermediate_processing(context: ScheduleEvaluationContext):
    """Schedule daily processing of intermediate analytics models."""
    return RunRequest(
        run_key=f"intermediate_models_{context.scheduled_execution_time.strftime('%Y_%m_%d')}",
        tags={
            "schedule": "daily_intermediate_processing",
            "priority": "high",
            "data_type": "intermediate"
        }
    )


# Data Quality and Health Check Schedules  
@schedule(
    cron_schedule="0 12 * * *",  # Daily at noon
    job_name="nfl_data_health_check_job",
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_data_health_check(context: ScheduleEvaluationContext):
    """Schedule daily health checks for all NFL data assets."""
    return RunRequest(
        run_key=f"health_check_{context.scheduled_execution_time.strftime('%Y_%m_%d')}",
        tags={
            "schedule": "daily_health_check",
            "priority": "monitoring",
            "data_type": "validation"
        }
    )


@schedule(
    cron_schedule="0 10 * * 0",  # Weekly on Sunday at 10 AM
    job_name="nfl_staging_validation_job",
    default_status=DefaultScheduleStatus.RUNNING,
)
def weekly_staging_validation(context: ScheduleEvaluationContext):
    """Schedule comprehensive weekly validation of all staging models."""
    return RunRequest(
        run_key=f"staging_validation_{context.scheduled_execution_time.strftime('%Y_%m_%d')}",
        tags={
            "schedule": "weekly_staging_validation",
            "priority": "validation",
            "data_type": "quality_check"
        }
    )


# Conditional/Seasonal Schedules
@schedule(
    cron_schedule="0 4 * * 2",  # Weekly on Tuesday at 4 AM (during NFL season)
    job_name="nfl_seasonal_intensive_job",
    default_status=DefaultScheduleStatus.STOPPED,  # Enable during NFL season
)
def nfl_season_intensive_processing(context: ScheduleEvaluationContext):
    """
    Intensive processing schedule during NFL season (September-February).
    Includes all priority levels of data extraction and processing.
    """
    current_month = context.scheduled_execution_time.month
    
    # NFL season months (September through February)
    nfl_season_months = [9, 10, 11, 12, 1, 2]
    
    if current_month in nfl_season_months:
        return RunRequest(
            run_key=f"seasonal_intensive_{context.scheduled_execution_time.strftime('%Y_%m_%d')}",
            tags={
                "schedule": "nfl_season_intensive",
                "priority": "seasonal",
                "data_type": "comprehensive",
                "season_active": "true"
            }
        )
    else:
        return SkipReason("Outside of NFL season months")


# Emergency/Manual Schedules
@schedule(
    cron_schedule="0 0 1 * *",  # Monthly on the 1st at midnight
    job_name="nfl_full_pipeline_refresh_job", 
    default_status=DefaultScheduleStatus.STOPPED,  # Manual activation only
)
def monthly_full_pipeline_refresh(context: ScheduleEvaluationContext):
    """
    Monthly full pipeline refresh for data backfill and quality assurance.
    Should be manually triggered as needed.
    """
    return RunRequest(
        run_key=f"full_refresh_{context.scheduled_execution_time.strftime('%Y_%m')}",
        tags={
            "schedule": "monthly_full_refresh",
            "priority": "comprehensive",
            "data_type": "backfill",
            "manual_trigger": "true"
        }
    )