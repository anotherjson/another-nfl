"""Raw data extraction assets using the existing CLI tools."""

import subprocess
from pathlib import Path
from dagster import asset, AssetExecutionContext, get_dagster_logger
from datetime import datetime


@asset(
    description="Extract NFL play-by-play data for recent seasons",
    group_name="raw_data_extraction"
)
def pbp_data(context: AssetExecutionContext) -> dict:
    """Extract play-by-play data using the CLI tool."""
    logger = get_dagster_logger()
    current_year = datetime.now().year
    
    # Extract last 3 seasons of data
    years = [current_year - 2, current_year - 1, current_year]
    
    results = {}
    for year in years:
        logger.info(f"Extracting pbp data for {year}")
        
        try:
            result = subprocess.run([
                "uv", "run", "python", "-m", "src.cli",
                "extract", "dataset", "pbp", 
                "--year", str(year),
                "--verbose"
            ], capture_output=True, text=True, check=True)
            
            results[year] = {
                "success": True,
                "output": result.stdout
            }
            logger.info(f"Successfully extracted pbp data for {year}")
            
        except subprocess.CalledProcessError as e:
            results[year] = {
                "success": False,
                "error": e.stderr
            }
            logger.error(f"Failed to extract pbp data for {year}: {e.stderr}")
    
    return results


@asset(
    description="Extract NFL weekly player statistics",
    group_name="raw_data_extraction"
)
def weekly_data(context: AssetExecutionContext) -> dict:
    """Extract weekly player statistics using the CLI tool."""
    logger = get_dagster_logger()
    current_year = datetime.now().year
    
    # Extract using incremental processing
    try:
        result = subprocess.run([
            "uv", "run", "python", "-m", "src.cli",
            "extract", "incremental", "weekly",
            "--years", f"{current_year-2},{current_year-1},{current_year}",
            "--max-age-days", "7",
            "--verbose"
        ], capture_output=True, text=True, check=True)
        
        logger.info("Successfully extracted weekly data using incremental processing")
        return {
            "success": True,
            "output": result.stdout
        }
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to extract weekly data: {e.stderr}")
        return {
            "success": False,
            "error": e.stderr
        }


@asset(
    description="Extract NFL team descriptions (reference data)",
    group_name="raw_data_extraction"
)
def team_desc_data(context: AssetExecutionContext) -> dict:
    """Extract team description data using the CLI tool."""
    logger = get_dagster_logger()
    
    try:
        result = subprocess.run([
            "uv", "run", "python", "-m", "src.cli",
            "extract", "dataset", "team_desc",
            "--verbose"
        ], capture_output=True, text=True, check=True)
        
        logger.info("Successfully extracted team description data")
        return {
            "success": True,
            "output": result.stdout
        }
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to extract team description data: {e.stderr}")
        return {
            "success": False,
            "error": e.stderr
        }


@asset(
    description="Extract NFL game schedules",
    group_name="raw_data_extraction"
)
def schedules_data(context: AssetExecutionContext) -> dict:
    """Extract NFL schedules data using the CLI tool."""
    logger = get_dagster_logger()
    current_year = datetime.now().year
    
    try:
        result = subprocess.run([
            "uv", "run", "python", "-m", "src.cli",
            "extract", "incremental", "schedules",
            "--years", f"{current_year-2},{current_year-1},{current_year}",
            "--max-age-days", "1",
            "--verbose"
        ], capture_output=True, text=True, check=True)
        
        logger.info("Successfully extracted schedules data")
        return {
            "success": True,
            "output": result.stdout
        }
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to extract schedules data: {e.stderr}")
        return {
            "success": False,
            "error": e.stderr
        }