"""DuckLake-enhanced NFL data assets for Dagster pipeline."""

import os
from pathlib import Path
from datetime import datetime
from dagster import asset, AssetExecutionContext, get_dagster_logger
from nfl_dagster.resources.ducklake_resource import DuckLakeResource


@asset(
    group_name="ducklake_registration",
    description="Register existing NFL data with DuckLake catalog"
)
def ducklake_catalog_registration(context: AssetExecutionContext, ducklake: DuckLakeResource):
    """Register all existing NFL Parquet files with DuckLake catalog."""
    logger = get_dagster_logger()
    
    # Initialize DuckLake catalog
    ducklake.initialize_ducklake_catalog()
    logger.info("DuckLake catalog initialized")
    
    data_path = Path(ducklake.data_path)
    parquet_files = list(data_path.rglob("*.parquet"))
    
    registered_count = 0
    
    for parquet_file in parquet_files:
        try:
            # Skip sample files
            if "sample" in str(parquet_file):
                continue
                
            # Parse file path to extract dataset info
            relative_path = parquet_file.relative_to(data_path)
            path_parts = relative_path.parts
            
            if len(path_parts) >= 3 and path_parts[1].isdigit():
                # Year-based dataset
                dataset_name = path_parts[0]
                year = path_parts[1] 
                etl_date_part = path_parts[2]
                etl_date = etl_date_part.replace("etl_date=", "")
                schema_name = "nfl_raw"
                table_name = f"{dataset_name}_{year}"
            elif len(path_parts) >= 2:
                # Non-year dataset
                dataset_name = path_parts[0]
                etl_date_part = path_parts[1]
                etl_date = etl_date_part.replace("etl_date=", "")
                schema_name = "nfl_raw"
                table_name = dataset_name
            else:
                continue
            
            # Create metadata
            metadata = {
                "dataset_name": dataset_name,
                "file_format": "parquet",
                "compression": "snappy",
                "source": "nfl_data_py",
                "registered_by": "dagster"
            }
            
            if "year" in locals():
                metadata["year"] = int(year)
            
            # Register with DuckLake
            result = ducklake.register_table(
                schema_name=schema_name,
                table_name=table_name,
                file_path=str(parquet_file),
                etl_date=etl_date,
                metadata=metadata
            )
            
            logger.info(f"Registered {schema_name}.{table_name} version {result['version_number']}")
            registered_count += 1
            
        except Exception as e:
            logger.error(f"Failed to register {parquet_file}: {e}")
    
    return {
        "registered_files": registered_count,
        "catalog_status": "initialized",
        "registration_timestamp": datetime.now().isoformat()
    }


@asset(
    deps=["ducklake_catalog_registration"],
    group_name="ducklake_time_travel",
    description="Demonstrate DuckLake time travel capabilities"
)
def ducklake_time_travel_demo(context: AssetExecutionContext, ducklake: DuckLakeResource):
    """Demonstrate DuckLake time travel functionality with NFL data."""
    logger = get_dagster_logger()
    
    try:
        # Get all versions of team_desc
        versions = ducklake.get_table_versions("nfl_raw", "team_desc")
        logger.info(f"Found {len(versions)} versions of team_desc")
        
        # Query latest version using time travel
        df = ducklake.time_travel_query("nfl_raw", "team_desc", "2025-07-21")
        logger.info(f"Time travel query returned {len(df)} teams")
        
        # Sample team data
        sample_team = df.iloc[0] if not df.empty else None
        
        return {
            "versions_available": len(versions),
            "latest_query_rows": len(df),
            "sample_team": {
                "name": sample_team["team_name"] if sample_team is not None else None,
                "abbr": sample_team["team_abbr"] if sample_team is not None else None
            },
            "time_travel_status": "working",
            "query_timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Time travel demo failed: {e}")
        return {
            "time_travel_status": "failed",
            "error": str(e),
            "query_timestamp": datetime.now().isoformat()
        }


@asset(
    deps=["ducklake_catalog_registration"],
    group_name="ducklake_analytics", 
    description="Analytics queries using DuckLake catalog"
)
def ducklake_nfl_analytics(context: AssetExecutionContext, ducklake: DuckLakeResource):
    """Perform analytics queries using DuckLake time travel."""
    logger = get_dagster_logger()
    
    analytics_results = {}
    
    try:
        # Get catalog summary
        with ducklake.get_postgres_connection() as pg_conn:
            with pg_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT t.schema_name, t.table_name, COUNT(tv.version_number) as versions,
                           MAX(tv.etl_date) as latest_date, SUM(tv.row_count) as total_rows
                    FROM ducklake_catalog.tables t
                    LEFT JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
                    GROUP BY t.schema_name, t.table_name
                    ORDER BY total_rows DESC;
                """)
                
                catalog_summary = []
                for schema, table, versions, latest_date, total_rows in cursor.fetchall():
                    catalog_summary.append({
                        "schema": schema,
                        "table": table,
                        "versions": versions,
                        "latest_date": str(latest_date),
                        "total_rows": total_rows or 0
                    })
        
        analytics_results["catalog_summary"] = catalog_summary
        
        # Calculate total data managed by DuckLake
        total_rows = sum(item["total_rows"] for item in catalog_summary)
        total_tables = len(catalog_summary)
        
        analytics_results["totals"] = {
            "total_tables": total_tables,
            "total_rows": total_rows,
            "total_versions": sum(item["versions"] for item in catalog_summary)
        }
        
        logger.info(f"DuckLake managing {total_tables} tables with {total_rows:,} total rows")
        
        return analytics_results
        
    except Exception as e:
        logger.error(f"Analytics query failed: {e}")
        return {
            "analytics_status": "failed",
            "error": str(e),
            "query_timestamp": datetime.now().isoformat()
        }