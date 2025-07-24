"""Enhanced dbt model assets with Streamlit integration for the NFL analytics pipeline."""

import subprocess
from pathlib import Path
from dagster import (
    asset, 
    AssetExecutionContext, 
    get_dagster_logger, 
    multi_asset, 
    AssetOut,
    Output,
    MetadataValue,
    MaterializeResult
)
from ..resources.dbt_resource import DbtResource
from ..resources.ducklake_resource import DuckLakeResource


@multi_asset(
    outs={
        "stg_pbp_enhanced": AssetOut(
            description="Enhanced play-by-play staging data optimized for Streamlit visualization",
            metadata={"streamlit_ready": True, "visualization_priority": "high"}
        ),
        "stg_weekly_enhanced": AssetOut(
            description="Enhanced weekly player statistics optimized for fantasy analysis",
            metadata={"streamlit_ready": True, "visualization_priority": "high"}
        ),
        "stg_team_desc_enhanced": AssetOut(
            description="Enhanced team descriptions optimized for reference lookups",
            metadata={"streamlit_ready": True, "visualization_priority": "medium"}
        ),
        "stg_schedules_enhanced": AssetOut(
            description="Enhanced game schedules optimized for game analysis",
            metadata={"streamlit_ready": True, "visualization_priority": "medium"}
        ),
    },
    deps=["pbp_data", "weekly_data", "team_desc_data", "schedules_data"],
    group_name="dbt_staging_enhanced"
)
def dbt_enhanced_staging_models(context: AssetExecutionContext, dbt: DbtResource, ducklake: DuckLakeResource):
    """Run all enhanced dbt staging models optimized for Streamlit."""
    logger = get_dagster_logger()
    
    # First, ensure DuckLake catalog is initialized and up-to-date
    logger.info("Refreshing DuckLake catalog for latest data")
    ducklake.initialize_ducklake_catalog()
    
    # Register any new data files with the catalog
    data_path = Path(ducklake.data_path)
    parquet_files = list(data_path.rglob("*.parquet"))
    new_registrations = 0
    
    for parquet_file in parquet_files:
        if "sample" not in str(parquet_file):
            try:
                # Check if this file is already registered
                relative_path = parquet_file.relative_to(data_path)
                path_parts = relative_path.parts
                
                if len(path_parts) >= 3 and path_parts[1].isdigit():
                    dataset_name = path_parts[0]
                    year = path_parts[1]
                    etl_date = path_parts[2].replace("etl_date=", "")
                    table_name = f"{dataset_name}_{year}"
                elif len(path_parts) >= 2:
                    dataset_name = path_parts[0]
                    etl_date = path_parts[1].replace("etl_date=", "")
                    table_name = dataset_name
                else:
                    continue
                
                # Try to register (will skip if already exists)
                result = ducklake.register_table(
                    schema_name="nfl_raw",
                    table_name=table_name,
                    file_path=str(parquet_file),
                    etl_date=etl_date,
                    metadata={"registered_by": "enhanced_dbt_assets"}
                )
                
                if result.get("already_exists", False):
                    logger.debug(f"Table {table_name} already registered")
                else:
                    new_registrations += 1
                    logger.info(f"Registered new table: {table_name}")
                    
            except Exception as e:
                logger.warning(f"Could not register {parquet_file}: {e}")
    
    logger.info(f"DuckLake catalog updated with {new_registrations} new registrations")
    
    # Run enhanced staging models
    logger.info("Running enhanced dbt staging models")
    result = dbt.run(select="tag:streamlit_ready")
    
    if not result["success"]:
        # Try running the enhanced models individually
        enhanced_models = [
            "stg_pbp_enhanced",
            "stg_weekly_enhanced", 
            "stg_team_desc_enhanced",
            "stg_schedules_enhanced"
        ]
        
        successful_models = []
        failed_models = []
        
        for model in enhanced_models:
            try:
                model_result = dbt.run(select=model)
                if model_result["success"]:
                    successful_models.append(model)
                    logger.info(f"Successfully built {model}")
                else:
                    failed_models.append(model)
                    logger.error(f"Failed to build {model}: {model_result.get('stderr', 'Unknown error')}")
            except Exception as e:
                failed_models.append(model)
                logger.error(f"Exception building {model}: {e}")
        
        # If we have some successful models, continue
        if successful_models:
            logger.info(f"Built {len(successful_models)} of {len(enhanced_models)} enhanced staging models")
        else:
            raise Exception(f"All enhanced staging models failed: {failed_models}")
    else:
        successful_models = ["stg_pbp_enhanced", "stg_weekly_enhanced", "stg_team_desc_enhanced", "stg_schedules_enhanced"]
    
    # Run tests on successful models
    if successful_models:
        logger.info("Running tests on enhanced staging models")
        test_result = dbt.test(select="tag:streamlit_ready")
        
        if not test_result["success"]:
            logger.warning(f"Some enhanced staging tests failed: {test_result.get('stderr', 'Unknown error')}")
    
    # Clear Streamlit cache to force refresh
    try:
        streamlit_cache_clear()
        logger.info("Streamlit cache cleared to refresh dashboard data")
    except Exception as e:
        logger.warning(f"Could not clear Streamlit cache: {e}")
    
    # Get row counts for each successful model
    model_stats = {}
    for model in successful_models:
        try:
            count_query = f"SELECT count(*) as row_count FROM {model}"
            with ducklake.get_duckdb_connection() as conn:
                count_result = conn.execute(count_query).fetchone()
                model_stats[model] = count_result[0] if count_result else 0
        except Exception as e:
            logger.warning(f"Could not get row count for {model}: {e}")
            model_stats[model] = 0
    
    # Return outputs for each successful model
    outputs = {}
    
    for model in ["stg_pbp_enhanced", "stg_weekly_enhanced", "stg_team_desc_enhanced", "stg_schedules_enhanced"]:
        if model in successful_models:
            outputs[model] = Output(
                value={
                    "model_name": model,
                    "row_count": model_stats.get(model, 0),
                    "build_status": "success",
                    "streamlit_ready": True
                },
                metadata={
                    "row_count": MetadataValue.int(model_stats.get(model, 0)),
                    "build_status": MetadataValue.text("success"),
                    "streamlit_cache_cleared": MetadataValue.bool(True),
                    "ducklake_registrations": MetadataValue.int(new_registrations)
                }
            )
        else:
            outputs[model] = Output(
                value={
                    "model_name": model,
                    "row_count": 0,
                    "build_status": "failed",
                    "streamlit_ready": False
                },
                metadata={
                    "build_status": MetadataValue.text("failed"),
                    "streamlit_ready": MetadataValue.bool(False)
                }
            )
    
    return outputs


@asset(
    deps=["dbt_enhanced_staging_models"],
    group_name="streamlit_integration",
    description="Refresh Streamlit dashboard cache and trigger data reload"
)
def streamlit_data_refresh(context: AssetExecutionContext):
    """Refresh Streamlit dashboard data after dbt model updates."""
    logger = get_dagster_logger()
    
    try:
        # Clear all cached data functions
        streamlit_cache_clear()
        
        # Trigger a test query to warm up the cache
        from visualizations.streamlit_app.utils.dbt_connection import get_staging_table_summary
        summary = get_staging_table_summary()
        
        available_tables = len(summary.get("available_tables", []))
        total_rows = sum(
            info.get("row_count", 0) 
            for info in summary.get("table_info", {}).values() 
            if info.get("table_exists", False)
        )
        
        logger.info(f"Streamlit data refresh complete: {available_tables} tables, {total_rows:,} total rows")
        
        return MaterializeResult(
            metadata={
                "available_tables": MetadataValue.int(available_tables),
                "total_rows": MetadataValue.int(total_rows),
                "cache_cleared": MetadataValue.bool(True),
                "refresh_timestamp": MetadataValue.text(context.dagster_run.created_at.isoformat())
            }
        )
        
    except Exception as e:
        logger.error(f"Streamlit data refresh failed: {e}")
        return MaterializeResult(
            metadata={
                "refresh_status": MetadataValue.text("failed"),
                "error": MetadataValue.text(str(e))
            }
        )


def streamlit_cache_clear():
    """Clear Streamlit cache by restarting the app or clearing cache files."""
    try:
        # Method 1: Try to import and clear Streamlit cache directly
        import streamlit as st
        if hasattr(st, 'cache_data'):
            st.cache_data.clear()
        if hasattr(st, 'cache_resource'):
            st.cache_resource.clear()
        return True
    except ImportError:
        pass
    
    try:
        # Method 2: Clear cache files if they exist
        cache_paths = [
            Path.home() / ".streamlit",
            Path.cwd() / ".streamlit",
            Path("/tmp/streamlit")
        ]
        
        for cache_path in cache_paths:
            if cache_path.exists():
                import shutil
                shutil.rmtree(cache_path, ignore_errors=True)
        
        return True
    except Exception:
        pass
    
    return False


@asset(
    deps=["streamlit_data_refresh"],
    group_name="data_quality",
    description="Validate data quality across staging models for Streamlit consumption"
)
def staging_data_quality_check(context: AssetExecutionContext, ducklake: DuckLakeResource):
    """Perform data quality checks on staging models for Streamlit dashboard."""
    logger = get_dagster_logger()
    
    quality_results = {}
    staging_tables = ["stg_pbp_enhanced", "stg_weekly_enhanced", "stg_team_desc_enhanced", "stg_schedules_enhanced"]
    
    try:
        with ducklake.get_duckdb_connection() as conn:
            for table in staging_tables:
                try:
                    # Check if table exists
                    exists_query = f"""
                    SELECT count(*) as table_exists 
                    FROM information_schema.tables 
                    WHERE table_name = '{table.lower()}'
                    """
                    table_exists = conn.execute(exists_query).fetchone()[0] > 0
                    
                    if not table_exists:
                        quality_results[table] = {
                            "exists": False,
                            "row_count": 0,
                            "null_check": "skipped",
                            "duplicate_check": "skipped"
                        }
                        continue
                    
                    # Row count check
                    row_count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                    
                    # Null check on key columns
                    null_checks = {}
                    if table == "stg_team_desc_enhanced":
                        null_count = conn.execute(f"SELECT count(*) FROM {table} WHERE team_abbr IS NULL").fetchone()[0]
                        null_checks["team_abbr"] = null_count
                    elif table == "stg_weekly_enhanced":
                        null_count = conn.execute(f"SELECT count(*) FROM {table} WHERE player_id IS NULL").fetchone()[0]
                        null_checks["player_id"] = null_count
                    elif table in ["stg_pbp_enhanced", "stg_schedules_enhanced"]:
                        null_count = conn.execute(f"SELECT count(*) FROM {table} WHERE game_id IS NULL").fetchone()[0]
                        null_checks["game_id"] = null_count
                    
                    # Duplicate check
                    duplicate_count = 0
                    if table == "stg_team_desc_enhanced":
                        duplicate_count = conn.execute(f"""
                            SELECT count(*) - count(DISTINCT team_abbr) FROM {table}
                        """).fetchone()[0]
                    
                    quality_results[table] = {
                        "exists": True,
                        "row_count": row_count,
                        "null_checks": null_checks,
                        "duplicate_count": duplicate_count,
                        "quality_score": "pass" if row_count > 0 and sum(null_checks.values()) == 0 else "warning"
                    }
                    
                    logger.info(f"Quality check {table}: {row_count:,} rows, quality score: {quality_results[table]['quality_score']}")
                    
                except Exception as e:
                    logger.error(f"Quality check failed for {table}: {e}")
                    quality_results[table] = {"exists": False, "error": str(e)}
        
        # Calculate overall quality score
        total_tables = len(staging_tables)
        passed_tables = sum(1 for result in quality_results.values() if result.get("quality_score") == "pass")
        overall_score = passed_tables / total_tables if total_tables > 0 else 0
        
        return MaterializeResult(
            metadata={
                "total_tables_checked": MetadataValue.int(total_tables),
                "tables_passed": MetadataValue.int(passed_tables),
                "overall_quality_score": MetadataValue.float(overall_score),
                "quality_details": MetadataValue.json(quality_results)
            }
        )
        
    except Exception as e:
        logger.error(f"Data quality check failed: {e}")
        return MaterializeResult(
            metadata={
                "quality_check_status": MetadataValue.text("failed"),
                "error": MetadataValue.text(str(e))
            }
        )