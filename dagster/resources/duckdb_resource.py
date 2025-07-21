"""DuckDB resource for Dagster pipeline."""

import duckdb
from dagster import ConfigurableResource


class DuckDBResource(ConfigurableResource):
    """DuckDB resource for connecting to the NFL analytics database."""
    
    database_path: str = "../data/nfl_analytics.duckdb"
    
    def get_connection(self):
        """Get a DuckDB connection."""
        conn = duckdb.connect(self.database_path)
        
        # Install and load required extensions
        conn.execute("INSTALL httpfs")
        conn.execute("INSTALL parquet")
        conn.execute("LOAD httpfs")
        conn.execute("LOAD parquet")
        
        return conn
    
    def execute_query(self, query: str):
        """Execute a query and return results."""
        with self.get_connection() as conn:
            return conn.execute(query).fetchall()
    
    def execute_script(self, script: str):
        """Execute a SQL script."""
        with self.get_connection() as conn:
            conn.executescript(script)


# Resource instance
duckdb_resource = DuckDBResource()