"""dbt model assets for the NFL analytics pipeline."""

from dagster import asset, AssetExecutionContext, get_dagster_logger, multi_asset, AssetOut
from ..resources.dbt_resource import DbtResource


@multi_asset(
    outs={
        "stg_pbp": AssetOut(description="Staged play-by-play data"),
        "stg_weekly": AssetOut(description="Staged weekly player statistics"),
        "stg_team_desc": AssetOut(description="Staged team descriptions"),
        "stg_schedules": AssetOut(description="Staged game schedules"),
    },
    deps=["pbp_data", "weekly_data", "team_desc_data", "schedules_data"],
    group_name="dbt_staging"
)
def dbt_staging_models(context: AssetExecutionContext, dbt: DbtResource):
    """Run all dbt staging models."""
    logger = get_dagster_logger()
    
    logger.info("Running dbt staging models")
    result = dbt.run(select="tag:staging")
    
    if not result["success"]:
        raise Exception(f"dbt staging models failed: {result['stderr']}")
    
    # Run tests on staging models
    logger.info("Running tests on staging models")
    test_result = dbt.test(select="tag:staging")
    
    if not test_result["success"]:
        logger.warning(f"Some staging tests failed: {test_result['stderr']}")
    
    return (
        {"run_result": result, "models_built": ["stg_pbp"]},
        {"run_result": result, "models_built": ["stg_weekly"]}, 
        {"run_result": result, "models_built": ["stg_team_desc"]},
        {"run_result": result, "models_built": ["stg_schedules"]}
    )


@multi_asset(
    outs={
        "int_team_performance": AssetOut(description="Team performance metrics"),
        "int_player_weekly_stats": AssetOut(description="Weekly player statistics with rankings"),
    },
    deps=["stg_pbp", "stg_weekly", "stg_team_desc", "stg_schedules"],
    group_name="dbt_intermediate"
)
def dbt_intermediate_models(context: AssetExecutionContext, dbt: DbtResource):
    """Run all dbt intermediate models."""
    logger = get_dagster_logger()
    
    logger.info("Running dbt intermediate models")
    result = dbt.run(select="tag:intermediate")
    
    if not result["success"]:
        raise Exception(f"dbt intermediate models failed: {result['stderr']}")
    
    # Run tests on intermediate models
    logger.info("Running tests on intermediate models")
    test_result = dbt.test(select="tag:intermediate")
    
    if not test_result["success"]:
        logger.warning(f"Some intermediate tests failed: {test_result['stderr']}")
    
    return (
        {"run_result": result, "models_built": ["int_team_performance"]},
        {"run_result": result, "models_built": ["int_player_weekly_stats"]}
    )


@multi_asset(
    outs={
        "mart_weekly_team_stats": AssetOut(description="Weekly team statistics for analytics"),
        "mart_player_season_stats": AssetOut(description="Season player statistics for analytics"),
        "mart_game_results": AssetOut(description="Game results and matchup analysis"),
    },
    deps=["int_team_performance", "int_player_weekly_stats"],
    group_name="dbt_marts"
)
def dbt_marts_models(context: AssetExecutionContext, dbt: DbtResource):
    """Run all dbt marts models."""
    logger = get_dagster_logger()
    
    logger.info("Running dbt marts models") 
    result = dbt.run(select="tag:marts")
    
    if not result["success"]:
        raise Exception(f"dbt marts models failed: {result['stderr']}")
    
    # Run tests on marts models
    logger.info("Running tests on marts models")
    test_result = dbt.test(select="tag:marts")
    
    if not test_result["success"]:
        logger.warning(f"Some marts tests failed: {test_result['stderr']}")
    
    # Generate documentation
    logger.info("Generating dbt documentation")
    docs_result = dbt.docs_generate()
    
    return (
        {"run_result": result, "models_built": ["mart_weekly_team_stats"]},
        {"run_result": result, "models_built": ["mart_player_season_stats"]},
        {"run_result": result, "models_built": ["mart_game_results"]}
    )