#!/usr/bin/env python3
"""Test DuckLake integration with NFL data pipeline."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from nfl_dagster.resources.ducklake_resource import DuckLakeResource


def test_ducklake_integration():
    """Test complete DuckLake integration."""
    print("🧪 Testing DuckLake Integration for NFL Data Pipeline")
    print("=" * 60)
    
    ducklake = DuckLakeResource()
    
    # Test 1: PostgreSQL Connection
    print("1️⃣  Testing PostgreSQL catalog connection...")
    try:
        with ducklake.get_postgres_connection() as pg_conn:
            with pg_conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                print(f"   ✅ PostgreSQL: {version[:50]}...")
    except Exception as e:
        print(f"   ❌ PostgreSQL connection failed: {e}")
        return False
    
    # Test 2: DuckDB with DuckLake extension
    print("2️⃣  Testing DuckDB with DuckLake extension...")
    try:
        with ducklake.get_duckdb_connection() as duck_conn:
            result = duck_conn.execute("SELECT version();").fetchone()
            print(f"   ✅ DuckDB: {result[0][:50]}...")
    except Exception as e:
        print(f"   ❌ DuckDB connection failed: {e}")
        return False
    
    # Test 3: Catalog status
    print("3️⃣  Checking DuckLake catalog status...")
    try:
        with ducklake.get_postgres_connection() as pg_conn:
            with pg_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM ducklake_catalog.tables;
                """)
                table_count = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT COUNT(*) FROM ducklake_catalog.table_versions;
                """)
                version_count = cursor.fetchone()[0]
                
                print(f"   ✅ Catalog: {table_count} tables, {version_count} versions")
    except Exception as e:
        print(f"   ❌ Catalog check failed: {e}")
        return False
    
    # Test 4: Time travel functionality
    print("4️⃣  Testing time travel functionality...")
    try:
        versions = ducklake.get_table_versions("nfl_raw", "team_desc")
        print(f"   ✅ Found {len(versions)} versions of team_desc")
        
        if versions:
            df = ducklake.time_travel_query("nfl_raw", "team_desc", "2025-07-21")
            print(f"   ✅ Time travel query returned {len(df)} rows")
    except Exception as e:
        print(f"   ❌ Time travel test failed: {e}")
        return False
    
    # Test 5: Data summary
    print("5️⃣  DuckLake data summary...")
    try:
        with ducklake.get_postgres_connection() as pg_conn:
            with pg_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT t.table_name, COUNT(tv.version_number) as versions,
                           SUM(tv.row_count) as total_rows
                    FROM ducklake_catalog.tables t
                    LEFT JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
                    GROUP BY t.table_name
                    ORDER BY total_rows DESC;
                """)
                
                print("   📊 NFL Data in DuckLake:")
                for table, versions, total_rows in cursor.fetchall():
                    print(f"      📋 {table}: {versions} versions, {total_rows:,} rows")
    except Exception as e:
        print(f"   ❌ Data summary failed: {e}")
        return False
    
    print("\n🎉 DuckLake integration test completed successfully!")
    return True


if __name__ == "__main__":
    success = test_ducklake_integration()
    sys.exit(0 if success else 1)