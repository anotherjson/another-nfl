"""
Dagster jobs for NFL data pipeline orchestration.

This module defines job definitions that group related assets for scheduled execution.
"""

from dagster import (
    AssetSelection,
    define_asset_job,
    DefaultExecutorType,
)


# Raw Data Extraction Jobs
nfl_critical_raw_data_job = define_asset_job(
    name="nfl_critical_raw_data_job",
    description="Extract critical NFL datasets (pbp, weekly, schedules, team_desc)",
    selection=AssetSelection.keys(
        "pbp_raw", "weekly_raw", "schedules_raw", "team_desc_raw"
    ),
    executor_def=DefaultExecutorType.MULTIPROCESS,
    tags={
        "priority": "critical",
        "data_type": "raw_extraction",
        "team": "nfl_analytics"
    }
)

nfl_high_priority_raw_data_job = define_asset_job(
    name="nfl_high_priority_raw_data_job", 
    description="Extract high priority NFL datasets",
    selection=AssetSelection.keys(
        "seasonal_raw", "players_raw", "weekly_rosters_raw", "seasonal_rosters_raw"
    ),
    executor_def=DefaultExecutorType.MULTIPROCESS,
    tags={
        "priority": "high",
        "data_type": "raw_extraction",
        "team": "nfl_analytics"
    }
)

nfl_medium_priority_raw_data_job = define_asset_job(
    name="nfl_medium_priority_raw_data_job",
    description="Extract medium priority NFL datasets",
    selection=AssetSelection.keys(
        "qbr_raw", "injuries_raw", "depth_charts_raw", "snap_counts_raw", "ngs_data_raw"
    ),
    executor_def=DefaultExecutorType.MULTIPROCESS,
    tags={
        "priority": "medium",
        "data_type": "raw_extraction",
        "team": "nfl_analytics"
    }
)

nfl_low_priority_raw_data_job = define_asset_job(
    name="nfl_low_priority_raw_data_job",
    description="Extract low priority NFL datasets",
    selection=AssetSelection.keys(
        "weekly_pfr_raw", "seasonal_pfr_raw", "officials_raw", 
        "ftn_data_raw", "combine_raw", "draft_picks_raw"
    ),
    executor_def=DefaultExecutorType.MULTIPROCESS,
    tags={
        "priority": "low",
        "data_type": "raw_extraction",
        "team": "nfl_analytics"
    }
)


# Staging Model Processing Jobs
nfl_critical_staging_job = define_asset_job(
    name="nfl_critical_staging_job",
    description="Process critical dbt staging models",
    selection=AssetSelection.keys(
        "stg_pbp", "stg_weekly", "stg_schedules", "stg_team_desc"
    ),
    executor_def=DefaultExecutorType.IN_PROCESS,
    tags={
        "priority": "critical",
        "data_type": "staging_models",
        "team": "nfl_analytics"
    }
)

nfl_high_priority_staging_job = define_asset_job(
    name="nfl_high_priority_staging_job",
    description="Process high priority dbt staging models", 
    selection=AssetSelection.keys(
        "stg_seasonal", "stg_players", "stg_weekly_rosters", "stg_seasonal_rosters"
    ),
    executor_def=DefaultExecutorType.IN_PROCESS,
    tags={
        "priority": "high",
        "data_type": "staging_models",
        "team": "nfl_analytics"
    }
)

nfl_medium_priority_staging_job = define_asset_job(
    name="nfl_medium_priority_staging_job",
    description="Process medium priority dbt staging models",
    selection=AssetSelection.keys(
        "stg_qbr", "stg_injuries", "stg_depth_charts", "stg_snap_counts", "stg_ngs_data"
    ),
    executor_def=DefaultExecutorType.IN_PROCESS,
    tags={
        "priority": "medium", 
        "data_type": "staging_models",
        "team": "nfl_analytics"
    }
)


# Intermediate Model Processing Job
nfl_intermediate_models_job = define_asset_job(
    name="nfl_intermediate_models_job",
    description="Process intermediate analytics models (team performance, player stats)",
    selection=AssetSelection.keys("dbt_intermediate_models"),
    executor_def=DefaultExecutorType.IN_PROCESS,
    tags={
        "priority": "high",
        "data_type": "intermediate_models", 
        "team": "nfl_analytics"
    }
)


# Monitoring and Validation Jobs
nfl_data_health_check_job = define_asset_job(
    name="nfl_data_health_check_job",
    description="Comprehensive health check for all NFL raw data",
    selection=AssetSelection.keys("nfl_raw_data_health_check"),
    executor_def=DefaultExecutorType.IN_PROCESS,
    tags={
        "priority": "monitoring",
        "data_type": "health_check",
        "team": "nfl_analytics"
    }
)

nfl_staging_validation_job = define_asset_job(
    name="nfl_staging_validation_job",
    description="Comprehensive validation of all staging models",
    selection=AssetSelection.keys("dbt_staging_validation"),
    executor_def=DefaultExecutorType.IN_PROCESS,
    tags={
        "priority": "validation",
        "data_type": "quality_check",
        "team": "nfl_analytics"
    }
)


# Comprehensive Pipeline Jobs
nfl_full_critical_pipeline_job = define_asset_job(
    name="nfl_full_critical_pipeline_job",
    description="Complete critical pipeline: raw extraction → staging → intermediate",
    selection=AssetSelection.keys(
        # Critical raw data
        "pbp_raw", "weekly_raw", "schedules_raw", "team_desc_raw",
        # Critical staging
        "stg_pbp", "stg_weekly", "stg_schedules", "stg_team_desc", 
        # Intermediate models
        "dbt_intermediate_models"
    ),
    executor_def=DefaultExecutorType.MULTIPROCESS,
    tags={
        "priority": "critical",
        "data_type": "full_pipeline",
        "team": "nfl_analytics"
    }
)

nfl_seasonal_intensive_job = define_asset_job(
    name="nfl_seasonal_intensive_job", 
    description="Intensive processing during NFL season with all data sources",
    selection=AssetSelection.all(),  # All assets
    executor_def=DefaultExecutorType.MULTIPROCESS,
    tags={
        "priority": "seasonal",
        "data_type": "comprehensive",
        "team": "nfl_analytics"
    }
)

nfl_full_pipeline_refresh_job = define_asset_job(
    name="nfl_full_pipeline_refresh_job",
    description="Complete pipeline refresh for backfill and data quality assurance",
    selection=AssetSelection.all(),
    executor_def=DefaultExecutorType.MULTIPROCESS,
    tags={
        "priority": "comprehensive",
        "data_type": "backfill",
        "team": "nfl_analytics",
        "manual_trigger": "true"
    }
)