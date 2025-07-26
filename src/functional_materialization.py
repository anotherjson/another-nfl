"""
Pure functional materialization for NFL staging models via Dagster and dbt.

All functions are pure with no side effects, returning immutable CLIResult containers.
Materialization pipeline: config loading → asset creation → dbt execution → validation
"""

from __future__ import annotations

import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .functional_utils import (
    CLIResult,
    MaterializationResult,
    safe_call,
    compose,
    pipe,
    curry,
)


# Pure configuration functions
@safe_call
def load_dbt_project_config() -> dict[str, Any]:
    """Load dbt project configuration."""
    import yaml
    
    dbt_project_path = Path("dbt/dbt_project.yml")
    if not dbt_project_path.exists():
        raise FileNotFoundError("dbt_project.yml not found")
    
    with open(dbt_project_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


@safe_call
def get_dbt_models_list() -> list[str]:
    """Get list of available dbt models."""
    try:
        result = subprocess.run(
            ["uv", "run", "dbt", "list", "--models", "tag:staging"],
            cwd="dbt",
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"dbt list failed: {result.stderr}")
        
        # Parse model names from output
        models = []
        for line in result.stdout.strip().split('\n'):
            if line and not line.startswith('==') and '.' in line:
                # Extract model name from full path like "nfl_analytics.staging.stg_pbp"
                model_name = line.split('.')[-1]
                models.append(model_name)
        
        return models
    
    except subprocess.TimeoutExpired:
        raise RuntimeError("dbt list command timed out")


@safe_call
def get_dagster_asset_status() -> dict[str, Any]:
    """Get Dagster asset materialization status."""
    try:
        # Use dagster CLI to get asset status
        result = subprocess.run(
            ["uv", "run", "dagster", "asset", "list", "-f", "nfl_dagster/definitions.py"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"dagster asset list failed: {result.stderr}")
        
        # Parse asset information
        assets = {}
        current_asset = None
        
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if line and not line.startswith('=='):
                if line.startswith('nfl_'):
                    current_asset = line
                    assets[current_asset] = {"status": "unknown", "last_run": None}
                elif current_asset and "MATERIALIZED" in line:
                    assets[current_asset]["status"] = "materialized"
                elif current_asset and "NEVER MATERIALIZED" in line:
                    assets[current_asset]["status"] = "never_materialized"
        
        return assets
    
    except subprocess.TimeoutExpired:
        raise RuntimeError("dagster asset list command timed out")


# Pure materialization functions
@safe_call
def run_dbt_models(models: list[str] | None = None, select_tag: str | None = None) -> dict[str, Any]:
    """
    Pure function to run dbt models.
    
    Returns materialization results without side effects beyond dbt execution.
    """
    cmd = ["uv", "run", "dbt", "run"]
    
    if select_tag:
        cmd.extend(["--select", f"tag:{select_tag}"])
    elif models:
        cmd.extend(["--select"] + models)
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            cwd="dbt",
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        duration = time.time() - start_time
        
        # Parse dbt output for success/failure counts
        success_count = 0
        error_count = 0
        errors = []
        models_materialized = []
        
        output_lines = result.stdout.split('\n') + result.stderr.split('\n')
        
        for line in output_lines:
            if "completed successfully" in line.lower():
                success_count += 1
                # Extract model name if possible
                if "model" in line.lower():
                    for word in line.split():
                        if word.startswith("stg_") or word.startswith("int_"):
                            models_materialized.append(word)
            elif "error" in line.lower() or "fail" in line.lower():
                if line.strip():
                    errors.append(line.strip())
                    error_count += 1
        
        # If we couldn't parse specific counts, use return code
        if success_count == 0 and error_count == 0:
            if result.returncode == 0:
                success_count = len(models) if models else 1
            else:
                error_count = 1
                errors.append(result.stderr or "Unknown dbt error")
        
        return {
            "success_count": success_count,
            "error_count": error_count,
            "total_duration_seconds": round(duration, 2),
            "models_materialized": list(set(models_materialized)),  # Remove duplicates
            "errors": errors,
            "return_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    
    except subprocess.TimeoutExpired:
        return {
            "success_count": 0,
            "error_count": 1,
            "total_duration_seconds": time.time() - start_time,
            "models_materialized": [],
            "errors": ["dbt run command timed out (5 minutes)"],
            "return_code": -1,
            "stdout": "",
            "stderr": "Timeout"
        }


@safe_call
def test_dbt_models(models: list[str] | None = None, select_tag: str | None = None) -> dict[str, Any]:
    """
    Pure function to test dbt models.
    
    Returns test results without side effects.
    """
    cmd = ["uv", "run", "dbt", "test"]
    
    if select_tag:
        cmd.extend(["--select", f"tag:{select_tag}"])
    elif models:
        cmd.extend(["--select"] + models)
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            cwd="dbt",
            capture_output=True,
            text=True,
            timeout=180  # 3 minute timeout
        )
        
        duration = time.time() - start_time
        
        # Parse test results
        passed_tests = 0
        failed_tests = 0
        test_errors = []
        
        output_lines = result.stdout.split('\n') + result.stderr.split('\n')
        
        for line in output_lines:
            if "passed" in line.lower():
                passed_tests += 1
            elif "failed" in line.lower() or "error" in line.lower():
                if line.strip():
                    test_errors.append(line.strip())
                    failed_tests += 1
        
        return {
            "passed_tests": passed_tests,
            "failed_tests": failed_tests,
            "total_duration_seconds": round(duration, 2),
            "test_errors": test_errors,
            "return_code": result.returncode,
            "all_passed": result.returncode == 0 and failed_tests == 0
        }
    
    except subprocess.TimeoutExpired:
        return {
            "passed_tests": 0,
            "failed_tests": 1,
            "total_duration_seconds": time.time() - start_time,
            "test_errors": ["dbt test command timed out (3 minutes)"],
            "return_code": -1,
            "all_passed": False
        }


@safe_call
def materialize_dagster_assets(asset_keys: list[str] | None = None, select_pattern: str | None = None) -> dict[str, Any]:
    """
    Pure function to materialize Dagster assets.
    
    Returns materialization results.
    """
    cmd = ["uv", "run", "dagster", "asset", "materialize"]
    
    if select_pattern:
        cmd.extend(["--select", select_pattern])
    elif asset_keys:
        for key in asset_keys:
            cmd.extend(["--select", key])
    
    cmd.extend(["-f", "nfl_dagster/definitions.py"])
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout for asset materialization
        )
        
        duration = time.time() - start_time
        
        # Parse Dagster output
        successful_assets = []
        failed_assets = []
        errors = []
        
        output_lines = result.stdout.split('\n') + result.stderr.split('\n')
        
        for line in output_lines:
            if "successfully materialized" in line.lower():
                # Extract asset name if possible
                for word in line.split():
                    if "nfl_" in word or "stg_" in word or "dbt_" in word:
                        successful_assets.append(word)
            elif "failed" in line.lower() or "error" in line.lower():
                if line.strip():
                    errors.append(line.strip())
                    # Try to extract failed asset name
                    for word in line.split():
                        if "nfl_" in word or "stg_" in word or "dbt_" in word:
                            failed_assets.append(word)
        
        return {
            "successful_assets": list(set(successful_assets)),
            "failed_assets": list(set(failed_assets)),
            "total_duration_seconds": round(duration, 2),
            "errors": errors,
            "return_code": result.returncode,
            "materialization_successful": result.returncode == 0
        }
    
    except subprocess.TimeoutExpired:
        return {
            "successful_assets": [],
            "failed_assets": asset_keys or ["unknown"],
            "total_duration_seconds": time.time() - start_time,
            "errors": ["Dagster asset materialization timed out (10 minutes)"],
            "return_code": -1,
            "materialization_successful": False
        }


# Priority-based materialization functions
PRIORITY_MODELS = {
    'critical': ['stg_pbp', 'stg_weekly', 'stg_schedules', 'stg_team_desc'],
    'high': ['stg_seasonal', 'stg_players', 'stg_weekly_rosters', 'stg_seasonal_rosters'],
    'medium': ['stg_injuries', 'stg_depth_charts', 'stg_snap_counts', 'stg_qbr', 'stg_ngs_data'],
    'low': ['stg_weekly_pfr', 'stg_seasonal_pfr', 'stg_ftn_data', 'stg_officials', 'stg_combine', 'stg_draft_picks']
}

PRIORITY_ASSETS = {
    'critical': ['nfl_critical_raw_data', 'dbt_critical_staging_models'],
    'high': ['nfl_high_priority_raw_data', 'dbt_high_priority_staging_models'],
    'medium': ['nfl_medium_priority_raw_data', 'dbt_medium_priority_staging_models'],
    'low': ['nfl_low_priority_raw_data', 'dbt_low_priority_staging_models']
}


def materialize_by_priority(priority: str, include_tests: bool = True) -> CLIResult[MaterializationResult]:
    """
    Materialize models by priority using functional composition.
    
    Pipeline: validate priority → get models → run dbt → run tests → create result
    """
    if priority not in PRIORITY_MODELS:
        return CLIResult.error(f"Invalid priority: {priority}. Must be one of: {list(PRIORITY_MODELS.keys())}")
    
    models = PRIORITY_MODELS[priority]
    assets = PRIORITY_ASSETS.get(priority, [])
    
    start_time = time.time()
    all_errors = []
    
    # First, materialize Dagster assets if available
    dagster_result = None
    if assets:
        dagster_result = materialize_dagster_assets(assets)
        if not dagster_result.success:
            all_errors.append(f"Dagster materialization failed: {dagster_result.error}")
        elif dagster_result.data and dagster_result.data.get("errors"):
            all_errors.extend(dagster_result.data["errors"])
    
    # Then run dbt models
    dbt_result = run_dbt_models(models)
    if not dbt_result.success:
        all_errors.append(f"dbt run failed: {dbt_result.error}")
        return CLIResult.error(f"Materialization failed: {'; '.join(all_errors)}")
    
    dbt_data = dbt_result.data
    
    # Run tests if requested
    test_result = None
    if include_tests:
        test_result = test_dbt_models(models)
        if not test_result.success:
            all_errors.append(f"dbt test failed: {test_result.error}")
        elif test_result.data and not test_result.data.get("all_passed", False):
            test_errors = test_result.data.get("test_errors", [])
            all_errors.extend(test_errors)
    
    total_duration = time.time() - start_time
    
    # Combine results
    models_materialized = dbt_data.get("models_materialized", models)
    success_count = dbt_data.get("success_count", 0)
    error_count = dbt_data.get("error_count", 0)
    
    # Add any additional errors
    if dbt_data.get("errors"):
        all_errors.extend(dbt_data["errors"])
    
    result = MaterializationResult(
        models_materialized=tuple(models_materialized),
        success_count=success_count,
        error_count=error_count + len(all_errors),
        total_duration_seconds=round(total_duration, 2),
        errors=tuple(all_errors)
    )
    
    return CLIResult.ok(result)


def materialize_all_staging_models(include_tests: bool = True) -> CLIResult[MaterializationResult]:
    """
    Materialize all staging models using functional composition.
    
    Executes all priority levels in sequence.
    """
    start_time = time.time()
    all_results = []
    combined_errors = []
    total_success = 0
    total_errors = 0
    all_models_materialized = []
    
    # Process each priority level
    for priority in ['critical', 'high', 'medium', 'low']:
        result = materialize_by_priority(priority, include_tests=False)  # Skip individual tests
        
        if result.success and result.data:
            all_results.append(result.data)
            total_success += result.data.success_count
            total_errors += result.data.error_count
            all_models_materialized.extend(result.data.models_materialized)
            combined_errors.extend(result.data.errors)
        else:
            combined_errors.append(f"Priority {priority} failed: {result.error}")
            total_errors += 1
    
    # Run comprehensive tests at the end if requested
    if include_tests:
        test_result = test_dbt_models(select_tag="staging")
        if not test_result.success:
            combined_errors.append(f"Final testing failed: {test_result.error}")
            total_errors += 1
        elif test_result.data and not test_result.data.get("all_passed", False):
            test_errors = test_result.data.get("test_errors", [])
            combined_errors.extend(test_errors)
            total_errors += test_result.data.get("failed_tests", 0)
    
    total_duration = time.time() - start_time
    
    result = MaterializationResult(
        models_materialized=tuple(set(all_models_materialized)),  # Remove duplicates
        success_count=total_success,
        error_count=total_errors,
        total_duration_seconds=round(total_duration, 2),
        errors=tuple(combined_errors)
    )
    
    return CLIResult.ok(result)


@curry
def materialize_specific_models(model_names: list[str], include_tests: bool = True) -> CLIResult[MaterializationResult]:
    """
    Materialize specific models by name.
    
    Curried function for partial application.
    """
    if not model_names:
        return CLIResult.error("No models specified")
    
    start_time = time.time()
    
    # Run dbt for specific models
    dbt_result = run_dbt_models(model_names)
    if not dbt_result.success:
        return CLIResult.error(f"dbt run failed: {dbt_result.error}")
    
    dbt_data = dbt_result.data
    all_errors = list(dbt_data.get("errors", []))
    
    # Run tests if requested
    if include_tests:
        test_result = test_dbt_models(model_names)
        if not test_result.success:
            all_errors.append(f"dbt test failed: {test_result.error}")
        elif test_result.data and not test_result.data.get("all_passed", False):
            test_errors = test_result.data.get("test_errors", [])
            all_errors.extend(test_errors)
    
    total_duration = time.time() - start_time
    
    result = MaterializationResult(
        models_materialized=tuple(dbt_data.get("models_materialized", model_names)),
        success_count=dbt_data.get("success_count", 0),
        error_count=dbt_data.get("error_count", 0) + len(all_errors),
        total_duration_seconds=round(total_duration, 2),
        errors=tuple(all_errors)
    )
    
    return CLIResult.ok(result)


# Status and monitoring functions
@safe_call
def get_materialization_status() -> dict[str, Any]:
    """
    Get comprehensive materialization status.
    
    Combines dbt model status with Dagster asset status.
    """
    status = {
        "timestamp": datetime.now().isoformat(),
        "dbt_models": {},
        "dagster_assets": {},
        "summary": {}
    }
    
    # Get dbt model information
    try:
        models_result = get_dbt_models_list()
        if models_result.success:
            models = models_result.data
            status["dbt_models"] = {
                "total_models": len(models),
                "staging_models": [m for m in models if m.startswith("stg_")],
                "intermediate_models": [m for m in models if m.startswith("int_")],
                "available_models": models
            }
    except Exception as e:
        status["dbt_models"]["error"] = str(e)
    
    # Get Dagster asset status
    try:
        assets_result = get_dagster_asset_status()
        if assets_result.success:
            assets = assets_result.data
            materialized_count = sum(1 for asset in assets.values() if asset.get("status") == "materialized")
            
            status["dagster_assets"] = {
                "total_assets": len(assets),
                "materialized_assets": materialized_count,
                "never_materialized": len(assets) - materialized_count,
                "assets": assets
            }
    except Exception as e:
        status["dagster_assets"]["error"] = str(e)
    
    # Generate summary
    total_models = status["dbt_models"].get("total_models", 0)
    total_assets = status["dagster_assets"].get("total_assets", 0)
    materialized_assets = status["dagster_assets"].get("materialized_assets", 0)
    
    status["summary"] = {
        "total_components": total_models + total_assets,
        "dbt_models_available": total_models,
        "dagster_assets_available": total_assets,
        "dagster_assets_materialized": materialized_assets,
        "overall_health": "healthy" if materialized_assets > 0 else "needs_materialization"
    }
    
    return status


def get_model_lineage() -> CLIResult[dict[str, Any]]:
    """
    Get model lineage information from dbt.
    
    Pure function that returns dependency information.
    """
    try:
        result = subprocess.run(
            ["uv", "run", "dbt", "list", "--select", "tag:staging", "--output", "json"],
            cwd="dbt",
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            return CLIResult.error(f"dbt list failed: {result.stderr}")
        
        # Parse JSON output
        import json
        lines = result.stdout.strip().split('\n')
        models_info = []
        
        for line in lines:
            if line.strip():
                try:
                    model_info = json.loads(line)
                    models_info.append(model_info)
                except json.JSONDecodeError:
                    continue
        
        # Build lineage map
        lineage = {
            "models": models_info,
            "total_models": len(models_info),
            "dependencies": {}
        }
        
        # Extract dependencies from model info
        for model in models_info:
            model_name = model.get("name", "")
            depends_on = model.get("depends_on", {}).get("nodes", [])
            lineage["dependencies"][model_name] = depends_on
        
        return CLIResult.ok(lineage)
    
    except subprocess.TimeoutExpired:
        return CLIResult.error("dbt list command timed out")
    except Exception as e:
        return CLIResult.error(f"Failed to get model lineage: {str(e)}")


# Composed materialization pipelines
def full_materialization_pipeline(priority: str | None = None, include_tests: bool = True) -> CLIResult[dict[str, Any]]:
    """
    Complete materialization pipeline with status reporting.
    
    Pipeline: status check → materialization → validation → final status
    """
    start_time = time.time()
    
    # Get initial status
    initial_status_result = get_materialization_status()
    initial_status = initial_status_result.data if initial_status_result.success else {}
    
    # Perform materialization
    if priority:
        materialization_result = materialize_by_priority(priority, include_tests)
    else:
        materialization_result = materialize_all_staging_models(include_tests)
    
    if not materialization_result.success:
        return CLIResult.error(f"Materialization pipeline failed: {materialization_result.error}")
    
    materialization_data = materialization_result.data
    
    # Get final status
    final_status_result = get_materialization_status()
    final_status = final_status_result.data if final_status_result.success else {}
    
    total_duration = time.time() - start_time
    
    pipeline_result = {
        "pipeline_duration_seconds": round(total_duration, 2),
        "materialization_result": materialization_data,
        "initial_status": initial_status,
        "final_status": final_status,
        "pipeline_successful": materialization_data.error_count == 0,
        "timestamp": datetime.now().isoformat()
    }
    
    return CLIResult.ok(pipeline_result)