#!/usr/bin/env python3
"""Register existing NFL Parquet files with DuckLake catalog."""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from nfl_dagster.resources.ducklake_resource import DuckLakeResource


def register_existing_nfl_data():
    """Register all existing NFL Parquet files with DuckLake catalog."""
    ducklake = DuckLakeResource()
    data_path = Path(ducklake.data_path)
    
    print("🏈 Registering existing NFL data with DuckLake catalog...")
    
    # Find all parquet files in the data directory
    parquet_files = list(data_path.rglob("*.parquet"))
    
    registered_count = 0
    
    for parquet_file in parquet_files:
        try:
            # Parse the file path to extract dataset info
            relative_path = parquet_file.relative_to(data_path)
            path_parts = relative_path.parts
            
            # Skip sample files
            if "sample" in str(parquet_file):
                continue
                
            # Determine dataset name and ETL date
            if len(path_parts) >= 3 and path_parts[1].isdigit():
                # Year-based dataset: data/{dataset}/{year}/etl_date={date}/data.parquet
                dataset_name = path_parts[0]
                year = path_parts[1] 
                etl_date_part = path_parts[2]
                etl_date = etl_date_part.replace("etl_date=", "")
                schema_name = "nfl_raw"
                table_name = f"{dataset_name}_{year}"
            elif len(path_parts) >= 2:
                # Non-year dataset: data/{dataset}/etl_date={date}/data.parquet  
                dataset_name = path_parts[0]
                etl_date_part = path_parts[1]
                etl_date = etl_date_part.replace("etl_date=", "")
                schema_name = "nfl_raw"
                table_name = dataset_name
            else:
                print(f"⏭️  Skipping {parquet_file} - cannot parse path structure")
                continue
            
            # Create metadata
            metadata = {
                "dataset_name": dataset_name,
                "file_format": "parquet",
                "compression": "snappy",
                "source": "nfl_data_py"
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
            
            print(f"✅ Registered {schema_name}.{table_name} version {result['version_number']}")
            registered_count += 1
            
        except Exception as e:
            print(f"❌ Failed to register {parquet_file}: {e}")
    
    print(f"🎉 Registration complete! {registered_count} files registered with DuckLake catalog.")
    
    # Show catalog summary
    print("\n📊 DuckLake Catalog Summary:")
    try:
        with ducklake.get_postgres_connection() as pg_conn:
            with pg_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT t.schema_name, t.table_name, COUNT(tv.version_number) as versions,
                           MAX(tv.etl_date) as latest_date, SUM(tv.row_count) as total_rows
                    FROM ducklake_catalog.tables t
                    LEFT JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
                    GROUP BY t.schema_name, t.table_name
                    ORDER BY t.table_name;
                """)
                
                results = cursor.fetchall()
                for schema, table, versions, latest_date, total_rows in results:
                    print(f"  📋 {schema}.{table}: {versions} versions, latest: {latest_date}, rows: {total_rows:,}")
    
    except Exception as e:
        print(f"❌ Error getting catalog summary: {e}")


if __name__ == "__main__":
    register_existing_nfl_data()