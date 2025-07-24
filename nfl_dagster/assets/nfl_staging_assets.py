"""
NFL dbt staging models as Dagster assets with proper dependencies.

This module wraps dbt staging models as Dagster assets, ensuring proper execution order
and dependency management from raw data extraction through staging layer processing.
"""

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetOut,
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
    multi_asset,
)

from ..resources.dbt_resource import DbtResource
from ..resources.ducklake_resource import DuckLakeResource


def _run_dbt_models(
    models: List[str],
    context: AssetExecutionContext,
    dbt: DbtResource,
    ducklake: DuckLakeResource,
    tag: Optional[str] = None,
) -> Dict:
    """Run specific dbt models with proper error handling and metadata collection."""
    logger = get_dagster_logger()
    
    # Build dbt command
    cmd = ["uv", "run", "dbt", "run"]
    
    if tag:
        cmd.extend(["--select", f"tag:{tag}"])
    elif models:
        cmd.extend(["--select"] + models)
    
    cmd.extend(["--profiles-dir", "dbt"])
    
    try:
        # Run dbt command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            cwd=Path.cwd()
        )
        
        logger.info(f"Successfully ran dbt models: {models if models else f'tag:{tag}'}")
        
        # Parse dbt output for model statistics
        output_lines = result.stdout.split('\n')
        models_run = []
        for line in output_lines:
            if "OK created" in line or "OK replaced" in line:
                parts = line.split()
                model_name = parts[-1] if parts else "unknown"
                models_run.append(model_name)
        
        # Test the models
        test_cmd = ["uv", "run", "dbt", "test"]
        if tag:
            test_cmd.extend(["--select", f"tag:{tag}"])
        elif models:
            test_cmd.extend(["--select"] + models)
        test_cmd.extend(["--profiles-dir", "dbt"])
        
        test_result = subprocess.run(
            test_cmd,
            capture_output=True,
            text=True,
            check=False  # Don't fail on test failures
        )
        
        # Count test results
        test_lines = test_result.stdout.split('\n') if test_result.stdout else []
        tests_passed = sum(1 for line in test_lines if "PASS" in line)
        tests_failed = sum(1 for line in test_lines if "FAIL" in line)
        
        return {
            "success": True,
            "models_run": models_run,
            "run_output": result.stdout,
            "test_output": test_result.stdout,
            "tests_passed": tests_passed,
            "tests_failed": tests_failed,
            "execution_time": datetime.now().isoformat()
        }
        
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt run failed: {e.stderr}")
        return {
            "success": False,
            "error": e.stderr,
            "models": models if models else f"tag:{tag}",
            "execution_time": datetime.now().isoformat()
        }


@multi_asset(
    outs={
        "stg_pbp": AssetOut(
            description="Staging play-by-play data with EPA metrics and game context",
            metadata={"dbt_model": "stg_pbp", "layer": "staging", "priority": "critical"}
        ),
        "stg_weekly": AssetOut(
            description="Staging weekly player statistics for fantasy and performance analysis",
            metadata={"dbt_model": "stg_weekly", "layer": "staging", "priority": "critical"}
        ),
        "stg_schedules": AssetOut(
            description="Staging game schedules with results and metadata",
            metadata={"dbt_model": "stg_schedules", "layer": "staging", "priority": "critical"}
        ),
        "stg_team_desc": AssetOut(
            description="Staging team descriptions with conference and division info",
            metadata={"dbt_model": "stg_team_desc", "layer": "staging", "priority": "critical"}
        ),
    },
    ins={
        "pbp_raw": AssetIn("pbp_raw"),
        "weekly_raw": AssetIn("weekly_raw"),  
        "schedules_raw": AssetIn("schedules_raw"),
        "team_desc_raw": AssetIn("team_desc_raw"),
    },
    group_name="dbt_staging_critical"
)
def dbt_critical_staging_models(
    context: AssetExecutionContext,
    dbt: DbtResource,
    ducklake: DuckLakeResource,
    pbp_raw,
    weekly_raw,
    schedules_raw,
    team_desc_raw,
) -> Dict:
    """Run critical dbt staging models with dependencies on raw data."""
    logger = get_dagster_logger()
    
    # First ensure DuckLake catalog is refreshed with latest raw data
    logger.info("Refreshing DuckLake catalog before staging model execution")
    try:
        ducklake.initialize_ducklake_catalog()
    except Exception as e:
        logger.warning(f"DuckLake catalog refresh failed: {e}")
    
    # Define critical staging models
    critical_models = [
        "stg_pbp",
        "stg_weekly", 
        "stg_schedules",
        "stg_team_desc"
    ]
    
    # Run the models
    result = _run_dbt_models(
        models=critical_models,
        context=context,
        dbt=dbt,
        ducklake=ducklake
    )
    
    # Create outputs with metadata
    base_metadata = {
        "execution_success": MetadataValue.bool(result["success"]),
        "execution_time": MetadataValue.text(result["execution_time"]),
        "models_processed": MetadataValue.json(result.get("models_run", [])),
    }
    
    if result["success"]:
        base_metadata.update({
            "tests_passed": MetadataValue.int(result.get("tests_passed", 0)),
            "tests_failed": MetadataValue.int(result.get("tests_failed", 0)),
        })
    
    return {
        "stg_pbp": Output(
            {"model": "stg_pbp", "status": "success" if result["success"] else "failed"},
            metadata={**base_metadata, "source_dependency": "pbp_raw"}
        ),
        "stg_weekly": Output(
            {"model": "stg_weekly", "status": "success" if result["success"] else "failed"},
            metadata={**base_metadata, "source_dependency": "weekly_raw"}
        ),
        "stg_schedules": Output(
            {"model": "stg_schedules", "status": "success" if result["success"] else "failed"},
            metadata={**base_metadata, "source_dependency": "schedules_raw"}
        ),
        "stg_team_desc": Output(
            {"model": "stg_team_desc", "status": "success" if result["success"] else "failed"},
            metadata={**base_metadata, "source_dependency": "team_desc_raw"}
        ),
    }


@multi_asset(
    outs={
        "stg_seasonal": AssetOut(
            description="Staging seasonal player statistics",
            metadata={"dbt_model": "stg_seasonal", "layer": "staging", "priority": "high"}
        ),
        "stg_players": AssetOut(
            description="Staging player information and identifiers",
            metadata={"dbt_model": "stg_players", "layer": "staging", "priority": "high"}
        ),
        "stg_weekly_rosters": AssetOut(
            description="Staging weekly team rosters",
            metadata={"dbt_model": "stg_weekly_rosters", "layer": "staging", "priority": "high"}
        ),
        "stg_seasonal_rosters": AssetOut(
            description="Staging seasonal team rosters",
            metadata={"dbt_model": "stg_seasonal_rosters", "layer": "staging", "priority": "high"}
        ),
    },
    ins={
        "seasonal_raw": AssetIn("seasonal_raw"),
        "players_raw": AssetIn("players_raw"),
        "weekly_rosters_raw": AssetIn("weekly_rosters_raw"),
        "seasonal_rosters_raw": AssetIn("seasonal_rosters_raw"),
    },
    group_name="dbt_staging_high_priority"
)
def dbt_high_priority_staging_models(
    context: AssetExecutionContext,
    dbt: DbtResource,
    ducklake: DuckLakeResource,
    seasonal_raw,
    players_raw,
    weekly_rosters_raw,
    seasonal_rosters_raw,
) -> Dict:
    """Run high priority dbt staging models."""
    logger = get_dagster_logger()
    
    high_priority_models = [
        "stg_seasonal",
        "stg_players",
        "stg_weekly_rosters",
        "stg_seasonal_rosters"
    ]
    
    result = _run_dbt_models(
        models=high_priority_models,
        context=context,
        dbt=dbt,
        ducklake=ducklake
    )
    
    base_metadata = {
        "execution_success": MetadataValue.bool(result["success"]),
        "execution_time": MetadataValue.text(result["execution_time"]),
    }
    
    return {
        "stg_seasonal": Output(
            {"model": "stg_seasonal", "status": "success" if result["success"] else "failed"},
            metadata=base_metadata
        ),
        "stg_players": Output(
            {"model": "stg_players", "status": "success" if result["success"] else "failed"}, 
            metadata=base_metadata
        ),
        "stg_weekly_rosters": Output(
            {"model": "stg_weekly_rosters", "status": "success" if result["success"] else "failed"},
            metadata=base_metadata
        ),
        "stg_seasonal_rosters": Output(
            {"model": "stg_seasonal_rosters", "status": "success" if result["success"] else "failed"},
            metadata=base_metadata
        ),
    }


@multi_asset(
    outs={
        "stg_qbr": AssetOut(description="Staging weekly QBR data"),
        "stg_injuries": AssetOut(description="Staging player injury reports"),
        "stg_depth_charts": AssetOut(description="Staging team depth charts"),
        "stg_snap_counts": AssetOut(description="Staging player snap counts"),
        "stg_ngs_data": AssetOut(description="Staging Next Gen Stats data"),
    },
    ins={
        "qbr_raw": AssetIn("qbr_raw"),
        "injuries_raw": AssetIn("injuries_raw"),
        "depth_charts_raw": AssetIn("depth_charts_raw"),
        "snap_counts_raw": AssetIn("snap_counts_raw"),
        "ngs_data_raw": AssetIn("ngs_data_raw"),
    },
    group_name="dbt_staging_medium_priority"
)
def dbt_medium_priority_staging_models(
    context: AssetExecutionContext,
    dbt: DbtResource,
    ducklake: DuckLakeResource,
    qbr_raw,
    injuries_raw,
    depth_charts_raw,
    snap_counts_raw,
    ngs_data_raw,
) -> Dict:
    """Run medium priority dbt staging models."""
    
    medium_priority_models = [
        "stg_qbr",
        "stg_injuries", 
        "stg_depth_charts",
        "stg_snap_counts",
        "stg_ngs_data"
    ]
    
    result = _run_dbt_models(
        models=medium_priority_models,
        context=context,
        dbt=dbt,
        ducklake=ducklake
    )
    
    base_metadata = {
        "execution_success": MetadataValue.bool(result["success"]),
        "execution_time": MetadataValue.text(result["execution_time"]),
    }
    
    return {
        "stg_qbr": Output({"model": "stg_qbr", "status": "success" if result["success"] else "failed"}, metadata=base_metadata),
        "stg_injuries": Output({"model": "stg_injuries", "status": "success" if result["success"] else "failed"}, metadata=base_metadata),
        "stg_depth_charts": Output({"model": "stg_depth_charts", "status": "success" if result["success"] else "failed"}, metadata=base_metadata),
        "stg_snap_counts": Output({"model": "stg_snap_counts", "status": "success" if result["success"] else "failed"}, metadata=base_metadata),
        "stg_ngs_data": Output({"model": "stg_ngs_data", "status": "success" if result["success"] else "failed"}, metadata=base_metadata),
    }


@asset(
    description="Run all existing intermediate dbt models that depend on staging",
    ins={
        "stg_pbp": AssetIn("stg_pbp"),
        "stg_weekly": AssetIn("stg_weekly"),
        "stg_schedules": AssetIn("stg_schedules"),
        "stg_team_desc": AssetIn("stg_team_desc"),
    },
    group_name="dbt_intermediate"
)
def dbt_intermediate_models(
    context: AssetExecutionContext,
    dbt: DbtResource,
    ducklake: DuckLakeResource,
    stg_pbp,
    stg_weekly,
    stg_schedules,
    stg_team_desc,
) -> Dict:
    """Run intermediate dbt models (int_team_performance, int_player_weekly_stats)"""
    logger = get_dagster_logger()
    
    # Run intermediate models using the existing tag
    result = _run_dbt_models(
        models=None,
        context=context,
        dbt=dbt,
        ducklake=ducklake,
        tag="intermediate"
    )
    
    return {
        "success": result["success"],
        "models_run": result.get("models_run", []),
        "tests_passed": result.get("tests_passed", 0),
        "tests_failed": result.get("tests_failed", 0),
        "execution_time": result["execution_time"]
    }


@asset(
    description="Comprehensive validation of all staging models and dependencies",
    deps=[
        "stg_pbp", "stg_weekly", "stg_schedules", "stg_team_desc",
        "stg_seasonal", "stg_players", "stg_weekly_rosters", "stg_seasonal_rosters"
    ],
    group_name="dbt_staging_validation"
)
def dbt_staging_validation(context: AssetExecutionContext, ducklake: DuckLakeResource) -> Dict:
    """Validate all staging models and run comprehensive tests."""
    logger = get_dagster_logger()
    
    try:
        # Run all staging tests
        test_result = subprocess.run([
            "uv", "run", "dbt", "test",
            "--select", "tag:staging",
            "--profiles-dir", "dbt"
        ], capture_output=True, text=True, check=False)
        
        # Parse test results
        test_lines = test_result.stdout.split('\n') if test_result.stdout else []
        tests_passed = sum(1 for line in test_lines if "PASS" in line)
        tests_failed = sum(1 for line in test_lines if "FAIL" in line) 
        tests_total = tests_passed + tests_failed
        
        # Check model freshness
        freshness_result = subprocess.run([
            "uv", "run", "dbt", "source", "freshness",
            "--profiles-dir", "dbt"
        ], capture_output=True, text=True, check=False)
        
        validation_status = {
            "all_tests_passed": tests_failed == 0,
            "tests_passed": tests_passed,
            "tests_failed": tests_failed,
            "tests_total": tests_total,
            "success_rate": tests_passed / tests_total if tests_total > 0 else 0,
            "freshness_check": "completed",
            "validation_time": datetime.now().isoformat()
        }
        
        logger.info(f"Staging validation completed: {validation_status}")
        return validation_status
        
    except Exception as e:
        logger.error(f"Staging validation failed: {e}")
        return {
            "all_tests_passed": False,
            "error": str(e),
            "validation_time": datetime.now().isoformat()
        }