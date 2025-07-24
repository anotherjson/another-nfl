"""
Enhanced NFL raw data assets with comprehensive dataset coverage and DuckLake integration.

This module provides Dagster assets for all 19 NFL datasets with:
- Automatic DuckLake registration
- Year-based partitioning
- Incremental processing
- Error handling and monitoring
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

from ..resources.ducklake_resource import DuckLakeResource


# NFL Dataset Configuration
NFL_DATASETS = {
    # Critical datasets (always processed)
    "pbp": {
        "description": "Play-by-play data with EPA metrics",
        "start_year": 1999,
        "requires_year": True,
        "priority": "critical",
        "years_to_extract": 3,  # Last 3 years
    },
    "weekly": {
        "description": "Weekly player statistics",
        "start_year": 1999,
        "requires_year": True,
        "priority": "critical",
        "years_to_extract": 3,
    },
    "schedules": {
        "description": "Game schedules and results",
        "start_year": 1999,
        "requires_year": True,
        "priority": "critical",
        "years_to_extract": 3,
    },
    "team_desc": {
        "description": "Team descriptions and information",
        "start_year": None,
        "requires_year": False,
        "priority": "critical",
        "years_to_extract": 0,  # Static data
    },
    
    # High priority datasets
    "seasonal": {
        "description": "Seasonal player statistics",
        "start_year": 1999,
        "requires_year": True,
        "priority": "high",
        "years_to_extract": 3,
    },
    "players": {
        "description": "Player information",
        "start_year": None,
        "requires_year": False,
        "priority": "high",
        "years_to_extract": 0,
    },
    "weekly_rosters": {
        "description": "Weekly team rosters",
        "start_year": 1999,
        "requires_year": True,
        "priority": "high",
        "years_to_extract": 2,
    },
    "seasonal_rosters": {
        "description": "Seasonal team rosters",
        "start_year": 1999,
        "requires_year": True,
        "priority": "high",
        "years_to_extract": 2,
    },
    
    # Medium priority datasets
    "qbr": {
        "description": "Weekly QBR data",
        "start_year": 2006,
        "requires_year": True,
        "priority": "medium",
        "years_to_extract": 2,
    },
    "injuries": {
        "description": "Player injury reports",
        "start_year": 2009,
        "requires_year": True,
        "priority": "medium",
        "years_to_extract": 2,
    },
    "depth_charts": {
        "description": "Team depth charts",
        "start_year": 2001,
        "requires_year": True,
        "priority": "medium",
        "years_to_extract": 2,
    },
    "snap_counts": {
        "description": "Player snap counts",
        "start_year": 2012,
        "requires_year": True,
        "priority": "medium",
        "years_to_extract": 2,
    },
    "ngs_data": {
        "description": "Next Gen Stats data",
        "start_year": 2016,
        "requires_year": True,
        "priority": "medium",
        "years_to_extract": 2,
    },
    
    # Lower priority datasets
    "weekly_pfr": {
        "description": "Pro Football Reference weekly stats",
        "start_year": 1932,
        "requires_year": True,
        "priority": "low",
        "years_to_extract": 1,
    },
    "seasonal_pfr": {
        "description": "Pro Football Reference seasonal stats",
        "start_year": 1932,
        "requires_year": True,
        "priority": "low",
        "years_to_extract": 1,
    },
    "officials": {
        "description": "Game officials",
        "start_year": 2001,
        "requires_year": True,
        "priority": "low",
        "years_to_extract": 1,
    },
    "ftn_data": {
        "description": "Fantasy Points allowed data",
        "start_year": 2018,
        "requires_year": True,
        "priority": "low",
        "years_to_extract": 1,
    },
    "combine": {
        "description": "NFL Combine results",
        "start_year": 1987,
        "requires_year": True,
        "priority": "low",
        "years_to_extract": 1,
    },
    "draft_picks": {
        "description": "NFL Draft picks",
        "start_year": 1936,
        "requires_year": True,
        "priority": "low",
        "years_to_extract": 1,
    },
}


def _extract_nfl_dataset(
    dataset_name: str,
    years: Optional[List[int]] = None,
    ducklake: Optional[DuckLakeResource] = None,
) -> Dict:
    """Extract NFL dataset using CLI and register in DuckLake."""
    logger = get_dagster_logger()
    results = {}
    
    dataset_config = NFL_DATASETS[dataset_name]
    
    if dataset_config["requires_year"] and years:
        # Year-based dataset
        for year in years:
            logger.info(f"Extracting {dataset_name} data for {year}")
            
            try:
                result = subprocess.run([
                    "uv", "run", "python", "-m", "src.cli",
                    "extract", "dataset", dataset_name,
                    "--year", str(year),
                    "--verbose"
                ], capture_output=True, text=True, check=True)
                
                results[year] = {
                    "success": True,
                    "output": result.stdout,
                    "dataset": dataset_name
                }
                
                # Register in DuckLake if available
                if ducklake:
                    try:
                        table_name = f"{dataset_name}_{year}"
                        ducklake.register_table(
                            schema_name="nfl_raw",
                            table_name=table_name,
                            file_path=f"data/{dataset_name}/{year}/etl_date=latest/data.parquet",
                            etl_date=datetime.now().strftime("%Y-%m-%d"),
                            metadata={
                                "dataset": dataset_name,
                                "year": year,
                                "priority": dataset_config["priority"],
                                "registered_by": "nfl_raw_data_assets"
                            }
                        )
                        logger.info(f"Registered {table_name} in DuckLake catalog")
                    except Exception as e:
                        logger.warning(f"Failed to register {table_name} in DuckLake: {e}")
                
                logger.info(f"Successfully extracted {dataset_name} data for {year}")
                
            except subprocess.CalledProcessError as e:
                results[year] = {
                    "success": False,
                    "error": e.stderr,
                    "dataset": dataset_name
                }
                logger.error(f"Failed to extract {dataset_name} data for {year}: {e.stderr}")
    
    else:
        # Static dataset (no year required)
        logger.info(f"Extracting static {dataset_name} data")
        
        try:
            result = subprocess.run([
                "uv", "run", "python", "-m", "src.cli",
                "extract", "dataset", dataset_name,
                "--verbose"
            ], capture_output=True, text=True, check=True)
            
            results["static"] = {
                "success": True,
                "output": result.stdout,
                "dataset": dataset_name
            }
            
            # Register in DuckLake if available
            if ducklake:
                try:
                    ducklake.register_table(
                        schema_name="nfl_raw",
                        table_name=dataset_name,
                        file_path=f"data/{dataset_name}/etl_date=latest/data.parquet",
                        etl_date=datetime.now().strftime("%Y-%m-%d"),
                        metadata={
                            "dataset": dataset_name,
                            "type": "static",
                            "priority": dataset_config["priority"],
                            "registered_by": "nfl_raw_data_assets"
                        }
                    )
                    logger.info(f"Registered {dataset_name} in DuckLake catalog")
                except Exception as e:
                    logger.warning(f"Failed to register {dataset_name} in DuckLake: {e}")
            
            logger.info(f"Successfully extracted static {dataset_name} data")
            
        except subprocess.CalledProcessError as e:
            results["static"] = {
                "success": False,
                "error": e.stderr,
                "dataset": dataset_name
            }
            logger.error(f"Failed to extract static {dataset_name} data: {e.stderr}")
    
    return results


# Critical Dataset Assets
@multi_asset(
    outs={
        "pbp_raw": AssetOut(
            description="Raw play-by-play data with EPA metrics",
            metadata={"priority": "critical", "dataset_type": "yearly"}
        ),
        "weekly_raw": AssetOut(
            description="Raw weekly player statistics",
            metadata={"priority": "critical", "dataset_type": "yearly"}
        ),
        "schedules_raw": AssetOut(
            description="Raw game schedules and results",
            metadata={"priority": "critical", "dataset_type": "yearly"}
        ),
        "team_desc_raw": AssetOut(
            description="Raw team descriptions and information",
            metadata={"priority": "critical", "dataset_type": "static"}
        ),
    },
    group_name="nfl_raw_critical"
)
def nfl_critical_raw_data(context: AssetExecutionContext, ducklake: DuckLakeResource):
    """Extract critical NFL datasets required for core analytics."""
    logger = get_dagster_logger()
    current_year = datetime.now().year
    years = [current_year - 2, current_year - 1, current_year]
    
    # Extract critical datasets
    pbp_result = _extract_nfl_dataset("pbp", years, ducklake)
    weekly_result = _extract_nfl_dataset("weekly", years, ducklake)
    schedules_result = _extract_nfl_dataset("schedules", years, ducklake)
    team_desc_result = _extract_nfl_dataset("team_desc", None, ducklake)
    
    # Calculate success metrics
    total_extractions = len(years) * 3 + 1  # 3 yearly datasets + 1 static
    successful_extractions = sum([
        sum(1 for r in pbp_result.values() if r["success"]),
        sum(1 for r in weekly_result.values() if r["success"]),
        sum(1 for r in schedules_result.values() if r["success"]),
        1 if team_desc_result.get("static", {}).get("success", False) else 0
    ])
    
    success_rate = successful_extractions / total_extractions
    
    return {
        "pbp_raw": Output(
            pbp_result,
            metadata={
                "years_extracted": MetadataValue.json(list(pbp_result.keys())),
                "success_count": MetadataValue.int(sum(1 for r in pbp_result.values() if r["success"])),
                "total_count": MetadataValue.int(len(pbp_result)),
            }
        ),
        "weekly_raw": Output(
            weekly_result,
            metadata={
                "years_extracted": MetadataValue.json(list(weekly_result.keys())),
                "success_count": MetadataValue.int(sum(1 for r in weekly_result.values() if r["success"])),
                "total_count": MetadataValue.int(len(weekly_result)),
            }
        ),
        "schedules_raw": Output(
            schedules_result,
            metadata={
                "years_extracted": MetadataValue.json(list(schedules_result.keys())),
                "success_count": MetadataValue.int(sum(1 for r in schedules_result.values() if r["success"])),
                "total_count": MetadataValue.int(len(schedules_result)),
            }
        ),
        "team_desc_raw": Output(
            team_desc_result,
            metadata={
                "dataset_type": MetadataValue.text("static"),
                "success": MetadataValue.bool(team_desc_result.get("static", {}).get("success", False)),
                "extraction_time": MetadataValue.timestamp(datetime.now()),
            }
        ),
    }


@multi_asset(
    outs={
        "seasonal_raw": AssetOut(description="Raw seasonal player statistics"),
        "players_raw": AssetOut(description="Raw player information"),
        "weekly_rosters_raw": AssetOut(description="Raw weekly team rosters"),
        "seasonal_rosters_raw": AssetOut(description="Raw seasonal team rosters"),
    },
    group_name="nfl_raw_high_priority"
)
def nfl_high_priority_raw_data(context: AssetExecutionContext, ducklake: DuckLakeResource):
    """Extract high priority NFL datasets for enhanced analytics."""
    current_year = datetime.now().year
    years = [current_year - 2, current_year - 1, current_year]
    
    seasonal_result = _extract_nfl_dataset("seasonal", years, ducklake)
    players_result = _extract_nfl_dataset("players", None, ducklake)
    weekly_rosters_result = _extract_nfl_dataset("weekly_rosters", years[:2], ducklake)  # Last 2 years
    seasonal_rosters_result = _extract_nfl_dataset("seasonal_rosters", years[:2], ducklake)
    
    return {
        "seasonal_raw": Output(seasonal_result),
        "players_raw": Output(players_result),
        "weekly_rosters_raw": Output(weekly_rosters_result),
        "seasonal_rosters_raw": Output(seasonal_rosters_result),
    }


@multi_asset(
    outs={
        "qbr_raw": AssetOut(description="Raw weekly QBR data"),
        "injuries_raw": AssetOut(description="Raw player injury reports"),
        "depth_charts_raw": AssetOut(description="Raw team depth charts"),
        "snap_counts_raw": AssetOut(description="Raw player snap counts"),
        "ngs_data_raw": AssetOut(description="Raw Next Gen Stats data"),
    },
    group_name="nfl_raw_medium_priority"
)
def nfl_medium_priority_raw_data(context: AssetExecutionContext, ducklake: DuckLakeResource):
    """Extract medium priority NFL datasets for specialized analytics."""
    current_year = datetime.now().year
    years = [current_year - 1, current_year]  # Last 2 years
    
    qbr_result = _extract_nfl_dataset("qbr", years, ducklake)
    injuries_result = _extract_nfl_dataset("injuries", years, ducklake)
    depth_charts_result = _extract_nfl_dataset("depth_charts", years, ducklake)
    snap_counts_result = _extract_nfl_dataset("snap_counts", years, ducklake)
    ngs_data_result = _extract_nfl_dataset("ngs_data", years, ducklake)
    
    return {
        "qbr_raw": Output(qbr_result),
        "injuries_raw": Output(injuries_result),
        "depth_charts_raw": Output(depth_charts_result),
        "snap_counts_raw": Output(snap_counts_result),
        "ngs_data_raw": Output(ngs_data_result),
    }


@multi_asset(
    outs={
        "weekly_pfr_raw": AssetOut(description="Raw Pro Football Reference weekly stats"),
        "seasonal_pfr_raw": AssetOut(description="Raw Pro Football Reference seasonal stats"),
        "officials_raw": AssetOut(description="Raw game officials data"),
        "ftn_data_raw": AssetOut(description="Raw Fantasy Points allowed data"),
        "combine_raw": AssetOut(description="Raw NFL Combine results"),
        "draft_picks_raw": AssetOut(description="Raw NFL Draft picks"),
    },
    group_name="nfl_raw_low_priority"
)
def nfl_low_priority_raw_data(context: AssetExecutionContext, ducklake: DuckLakeResource):
    """Extract low priority NFL datasets for supplementary analytics."""
    current_year = datetime.now().year
    years = [current_year]  # Current year only
    
    weekly_pfr_result = _extract_nfl_dataset("weekly_pfr", years, ducklake)
    seasonal_pfr_result = _extract_nfl_dataset("seasonal_pfr", years, ducklake)
    officials_result = _extract_nfl_dataset("officials", years, ducklake)
    ftn_data_result = _extract_nfl_dataset("ftn_data", years, ducklake)
    combine_result = _extract_nfl_dataset("combine", years, ducklake)
    draft_picks_result = _extract_nfl_dataset("draft_picks", years, ducklake)
    
    return {
        "weekly_pfr_raw": Output(weekly_pfr_result),
        "seasonal_pfr_raw": Output(seasonal_pfr_result),
        "officials_raw": Output(officials_result),
        "ftn_data_raw": Output(ftn_data_result),
        "combine_raw": Output(combine_result),
        "draft_picks_raw": Output(draft_picks_result),
    }


@asset(
    description="Comprehensive health check and catalog validation for all NFL raw data",
    group_name="nfl_raw_monitoring",
    deps=[
        "pbp_raw", "weekly_raw", "schedules_raw", "team_desc_raw",
        "seasonal_raw", "players_raw", "weekly_rosters_raw", "seasonal_rosters_raw"
    ]
)
def nfl_raw_data_health_check(context: AssetExecutionContext, ducklake: DuckLakeResource) -> Dict:
    """Validate all NFL raw data extractions and DuckLake catalog integrity."""
    logger = get_dagster_logger()
    
    # Get catalog tables
    catalog_tables = ducklake.get_catalog_tables()
    
    # Count registered tables by priority
    priority_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    for table in catalog_tables:
        table_info = ducklake.get_table_info("nfl_raw", table["table_name"])
        if table_info and "metadata" in table_info:
            priority = table_info["metadata"].get("priority", "unknown")
            if priority in priority_counts:
                priority_counts[priority] += 1
    
    # Run health checks
    health_status = {
        "total_tables_registered": len(catalog_tables),
        "priority_breakdown": priority_counts,
        "ducklake_connection": True,
        "catalog_accessible": True,
        "timestamp": datetime.now().isoformat()
    }
    
    logger.info(f"NFL raw data health check completed: {health_status}")
    
    return health_status