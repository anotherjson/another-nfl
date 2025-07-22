"""DuckLake resource for Dagster pipeline with PostgreSQL catalog."""

import os
import json
import duckdb
import psycopg2
from dagster import ConfigurableResource, get_dagster_logger
from dotenv import load_dotenv
from typing import Optional, Dict, Any

# Load environment variables
load_dotenv()


class DuckLakeResource(ConfigurableResource):
    """DuckLake resource for NFL analytics with PostgreSQL catalog and Parquet storage."""
    
    # PostgreSQL catalog configuration
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5433"))
    postgres_database: str = os.getenv("POSTGRES_DATABASE", "nfl_ducklake")
    postgres_user: str = os.getenv("POSTGRES_USER", "nfl_user")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "nfl_password")
    
    # DuckDB configuration
    duckdb_path: str = os.getenv("DUCKDB_DATABASE_PATH", "data/nfl_analytics.duckdb")
    
    # Data storage configuration
    data_path: str = os.getenv("NFL_DATA_PATH", "data/")
    
    def get_postgres_connection(self):
        """Get a PostgreSQL connection for catalog operations."""
        return psycopg2.connect(
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_database,
            user=self.postgres_user,
            password=self.postgres_password
        )
    
    def get_duckdb_connection(self):
        """Get a DuckDB connection with DuckLake extension."""
        conn = duckdb.connect(self.duckdb_path)
        
        # Install and load required extensions
        try:
            conn.execute("INSTALL ducklake")
            conn.execute("LOAD ducklake")
            get_dagster_logger().info("DuckLake extension loaded successfully")
        except Exception as e:
            get_dagster_logger().warning(f"Could not load DuckLake extension: {e}")
        
        # Load other required extensions
        conn.execute("INSTALL httpfs")
        conn.execute("INSTALL parquet")
        conn.execute("LOAD httpfs")
        conn.execute("LOAD parquet")
        
        return conn
    
    def initialize_ducklake_catalog(self):
        """Initialize DuckLake catalog in PostgreSQL."""
        logger = get_dagster_logger()
        
        try:
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cursor:
                    # Create DuckLake catalog schema
                    cursor.execute("""
                        CREATE SCHEMA IF NOT EXISTS ducklake_catalog;
                    """)
                    
                    # Create tables table for tracking DuckLake tables
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS ducklake_catalog.tables (
                            table_id SERIAL PRIMARY KEY,
                            schema_name VARCHAR(255) NOT NULL,
                            table_name VARCHAR(255) NOT NULL,
                            storage_location TEXT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            metadata JSONB,
                            UNIQUE(schema_name, table_name)
                        );
                    """)
                    
                    # Create versions table for time travel
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS ducklake_catalog.table_versions (
                            version_id SERIAL PRIMARY KEY,
                            table_id INTEGER REFERENCES ducklake_catalog.tables(table_id),
                            version_number INTEGER NOT NULL,
                            file_path TEXT NOT NULL,
                            etl_date DATE NOT NULL,
                            row_count BIGINT,
                            file_size BIGINT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(table_id, version_number)
                        );
                    """)
                    
                    pg_conn.commit()
                    logger.info("DuckLake catalog initialized successfully")
                    
        except Exception as e:
            logger.error(f"Failed to initialize DuckLake catalog: {e}")
            raise
    
    def register_table(self, schema_name: str, table_name: str, file_path: str, 
                      etl_date: str, metadata: Optional[Dict[str, Any]] = None):
        """Register a new table or version in DuckLake catalog."""
        logger = get_dagster_logger()
        
        try:
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cursor:
                    # Convert metadata to JSON string
                    metadata_json = json.dumps(metadata) if metadata else None
                    
                    # Insert or update table
                    cursor.execute("""
                        INSERT INTO ducklake_catalog.tables (schema_name, table_name, storage_location, metadata)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (schema_name, table_name) 
                        DO UPDATE SET 
                            updated_at = CURRENT_TIMESTAMP,
                            metadata = EXCLUDED.metadata
                        RETURNING table_id;
                    """, (schema_name, table_name, self.data_path, metadata_json))
                    
                    table_id = cursor.fetchone()[0]
                    
                    # Get next version number
                    cursor.execute("""
                        SELECT COALESCE(MAX(version_number), 0) + 1 
                        FROM ducklake_catalog.table_versions 
                        WHERE table_id = %s;
                    """, (table_id,))
                    
                    version_number = cursor.fetchone()[0]
                    
                    # Get file stats
                    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                    
                    # Count rows using DuckDB
                    row_count = 0
                    try:
                        with self.get_duckdb_connection() as duck_conn:
                            result = duck_conn.execute(f"SELECT COUNT(*) FROM '{file_path}'").fetchone()
                            row_count = result[0] if result else 0
                    except Exception as e:
                        logger.warning(f"Could not count rows for {file_path}: {e}")
                    
                    # Insert version
                    cursor.execute("""
                        INSERT INTO ducklake_catalog.table_versions 
                        (table_id, version_number, file_path, etl_date, row_count, file_size)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (table_id, version_number, file_path, etl_date, row_count, file_size))
                    
                    pg_conn.commit()
                    logger.info(f"Registered {schema_name}.{table_name} version {version_number}")
                    
                    return {"table_id": table_id, "version_number": version_number}
                    
        except Exception as e:
            logger.error(f"Failed to register table {schema_name}.{table_name}: {e}")
            raise
    
    def get_table_versions(self, schema_name: str, table_name: str):
        """Get all versions of a table."""
        try:
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT tv.version_number, tv.file_path, tv.etl_date, 
                               tv.row_count, tv.file_size, tv.created_at
                        FROM ducklake_catalog.tables t
                        JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
                        WHERE t.schema_name = %s AND t.table_name = %s
                        ORDER BY tv.version_number DESC;
                    """, (schema_name, table_name))
                    
                    return cursor.fetchall()
                    
        except Exception as e:
            get_dagster_logger().error(f"Failed to get versions for {schema_name}.{table_name}: {e}")
            raise
    
    def time_travel_query(self, schema_name: str, table_name: str, as_of_date: str):
        """Query table as of a specific date (time travel)."""
        try:
            with self.get_postgres_connection() as pg_conn:
                with pg_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT tv.file_path
                        FROM ducklake_catalog.tables t
                        JOIN ducklake_catalog.table_versions tv ON t.table_id = tv.table_id
                        WHERE t.schema_name = %s AND t.table_name = %s 
                        AND tv.etl_date <= %s
                        ORDER BY tv.etl_date DESC
                        LIMIT 1;
                    """, (schema_name, table_name, as_of_date))
                    
                    result = cursor.fetchone()
                    if result:
                        file_path = result[0]
                        with self.get_duckdb_connection() as duck_conn:
                            return duck_conn.execute(f"SELECT * FROM '{file_path}'").fetchdf()
                    else:
                        raise ValueError(f"No version found for {schema_name}.{table_name} as of {as_of_date}")
                        
        except Exception as e:
            get_dagster_logger().error(f"Time travel query failed: {e}")
            raise


# Resource instance
ducklake_resource = DuckLakeResource()