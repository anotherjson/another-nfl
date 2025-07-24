#!/usr/bin/env python3
"""DuckLake integration health check script."""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import duckdb
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def check_postgres_catalog_connection():
    """Check PostgreSQL catalog connection."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5433")),
            database=os.getenv("POSTGRES_DATABASE", "nfl_ducklake"),
            user=os.getenv("POSTGRES_USER", "nfl_user"),
            password=os.getenv("POSTGRES_PASSWORD", "nfl_password")
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        conn.close()
        return "PostgreSQL catalog connection successful"
    except Exception as e:
        raise Exception(f"PostgreSQL connection failed: {e}")


def check_duckdb_ducklake_extension():
    """Check DuckDB DuckLake extension availability."""
    try:
        conn = duckdb.connect()
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")
        conn.close()
        return "DuckLake extension installed and loaded successfully"
    except Exception as e:
        raise Exception(f"DuckLake extension failed: {e}")


def check_table_registrations():
    """Check if tables are registered in DuckLake catalog."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5433")),
            database=os.getenv("POSTGRES_DATABASE", "nfl_ducklake"),
            user=os.getenv("POSTGRES_USER", "nfl_user"),
            password=os.getenv("POSTGRES_PASSWORD", "nfl_password")
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM ducklake_catalog.tables")
            table_count = cursor.fetchone()[0]
        conn.close()
        return f"Found {table_count} tables registered in DuckLake catalog"
    except Exception as e:
        raise Exception(f"Table registration check failed: {e}")


def check_time_travel_functionality():
    """Check time travel functionality."""
    try:
        from ducklake_manager import DuckLakeManager
        ducklake = DuckLakeManager()
        
        # Try to get table versions
        versions = ducklake.get_table_versions("nfl_raw", "team_desc")
        return f"Time travel check passed - found {len(versions)} versions"
    except Exception as e:
        raise Exception(f"Time travel functionality failed: {e}")


def check_dbt_integration():
    """Check dbt integration with DuckLake."""
    try:
        # Check if dbt profiles.yml has ducklake extension
        profiles_path = Path("dbt/profiles.yml")
        if profiles_path.exists():
            content = profiles_path.read_text()
            if "ducklake" in content:
                return "dbt profiles.yml configured for DuckLake"
            else:
                raise Exception("dbt profiles.yml missing DuckLake configuration")
        else:
            raise Exception("dbt profiles.yml not found")
    except Exception as e:
        raise Exception(f"dbt integration check failed: {e}")


def check_cli_model_queries():
    """Check CLI model queries."""
    try:
        from ducklake_manager import DuckLakeManager
        ducklake = DuckLakeManager()
        
        # Try to list models
        models = ducklake.list_available_models()
        return f"CLI model queries working - found {len(models)} models"
    except Exception as e:
        raise Exception(f"CLI model query check failed: {e}")


def main():
    """Run comprehensive DuckLake health checks."""
    
    checks = [
        check_postgres_catalog_connection,
        check_duckdb_ducklake_extension,
        check_table_registrations,
        check_time_travel_functionality,
        check_dbt_integration,
        check_cli_model_queries
    ]
    
    results = []
    for check in checks:
        try:
            result = check()
            results.append(f"✅ {check.__name__}: {result}")
        except Exception as e:
            results.append(f"❌ {check.__name__}: {e}")
    
    print("\n".join(results))
    return all("✅" in r for r in results)


if __name__ == "__main__":
    exit(0 if main() else 1)