"""
DuckDB connection utilities for querying dbt staging models in Streamlit.
"""

import streamlit as st
import duckdb
import pandas as pd
import os
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime


class DbtDuckDBConnection:
    """Manages DuckDB connections for querying dbt staging models."""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or "data/nfl_analytics.duckdb"
        self._connection = None
    
    @st.cache_resource
    def get_connection(_self):
        """Get or create a DuckDB connection with necessary extensions."""
        if _self._connection is None:
            _self._connection = duckdb.connect(_self.db_path)
            
            # Load required extensions
            try:
                _self._connection.execute("INSTALL httpfs")
                _self._connection.execute("LOAD httpfs")
                _self._connection.execute("INSTALL parquet")
                _self._connection.execute("LOAD parquet")
            except Exception as e:
                st.warning(f"Extension loading issue (may be okay): {e}")
        
        return _self._connection
    
    def close(self):
        """Close the DuckDB connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        try:
            conn = self.get_connection()
            return conn.execute(query).df()
        except Exception as e:
            st.error(f"Query execution failed: {e}")
            return pd.DataFrame()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in DuckDB."""
        try:
            conn = self.get_connection()
            result = conn.execute(f"""
                SELECT count(*) as cnt 
                FROM information_schema.tables 
                WHERE table_name = '{table_name.lower()}'
            """).fetchone()
            return result[0] > 0 if result else False
        except:
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table metadata including row count and column info."""
        try:
            conn = self.get_connection()
            
            # Get column information
            columns_df = conn.execute(f"DESCRIBE {table_name}").df()
            
            # Get row count
            row_count = conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            
            # Get sample data
            sample_df = conn.execute(f"SELECT * FROM {table_name} LIMIT 5").df()
            
            return {
                "columns": columns_df,
                "row_count": row_count,
                "sample_data": sample_df,
                "table_exists": True
            }
        except Exception as e:
            return {
                "error": str(e),
                "table_exists": False
            }
    
    def get_available_staging_tables(self) -> List[str]:
        """Get list of available staging tables."""
        staging_tables = [
            "stg_pbp_enhanced",
            "stg_weekly_enhanced", 
            "stg_team_desc_enhanced",
            "stg_schedules_enhanced"
        ]
        
        available_tables = []
        for table in staging_tables:
            if self.table_exists(table):
                available_tables.append(table)
        
        return available_tables
    
    def create_staging_tables_from_parquet(self) -> Dict[str, str]:
        """Create staging table views from parquet files if dbt tables don't exist."""
        results = {}
        
        try:
            conn = self.get_connection()
            
            # Create team descriptions view
            if not self.table_exists("stg_team_desc_enhanced"):
                team_query = """
                CREATE VIEW IF NOT EXISTS stg_team_desc_enhanced AS
                SELECT 
                    team_abbr,
                    team_name,
                    team_id,
                    team_conf as team_conference,
                    team_division,
                    team_color as primary_color,
                    team_color2 as secondary_color,
                    team_name as full_team_name,
                    CASE 
                        WHEN team_conf = 'AFC' THEN 'American Football Conference'
                        WHEN team_conf = 'NFC' THEN 'National Football Conference'
                        ELSE team_conf
                    END as conference_full_name,
                    team_division as division_display,
                    COALESCE(team_color, '#808080') as chart_color_primary,
                    COALESCE(team_logo_espn, team_logo_wikipedia) as display_logo,
                    current_timestamp as dbt_loaded_at,
                    'fallback_view' as dbt_model
                FROM 'data/team_desc/etl_date=*/data.parquet'
                """
                conn.execute(team_query)
                results["stg_team_desc_enhanced"] = "Created from parquet"
            
            # Create weekly stats view
            if not self.table_exists("stg_weekly_enhanced"):
                weekly_query = """
                CREATE VIEW IF NOT EXISTS stg_weekly_enhanced AS
                SELECT 
                    player_id,
                    player_display_name as player_name,
                    position,
                    recent_team as team,
                    season,
                    week,
                    CASE 
                        WHEN position IN ('QB') THEN 'Quarterback'
                        WHEN position IN ('RB', 'FB') THEN 'Running Back'
                        WHEN position IN ('WR') THEN 'Wide Receiver'
                        WHEN position IN ('TE') THEN 'Tight End'
                        WHEN position IN ('K') THEN 'Kicker'
                        WHEN position IN ('DEF') THEN 'Defense'
                        ELSE 'Other'
                    END as position_group,
                    COALESCE(fantasy_points_ppr, 0) as fantasy_points_ppr,
                    CASE
                        WHEN COALESCE(fantasy_points_ppr, 0) >= 20 THEN 'Elite (20+)'
                        WHEN COALESCE(fantasy_points_ppr, 0) >= 15 THEN 'Great (15-19.9)'
                        WHEN COALESCE(fantasy_points_ppr, 0) >= 10 THEN 'Good (10-14.9)'
                        WHEN COALESCE(fantasy_points_ppr, 0) >= 5 THEN 'Okay (5-9.9)'
                        WHEN COALESCE(fantasy_points_ppr, 0) > 0 THEN 'Poor (0.1-4.9)'
                        ELSE 'Zero (0)'
                    END as fantasy_performance_tier,
                    COALESCE(passing_yards, 0) + COALESCE(rushing_yards, 0) + COALESCE(receiving_yards, 0) as total_yards,
                    COALESCE(passing_tds, 0) + COALESCE(rushing_tds, 0) + COALESCE(receiving_tds, 0) as total_touchdowns,
                    current_timestamp as dbt_loaded_at,
                    'fallback_view' as dbt_model
                FROM 'data/weekly/*/etl_date=*/data.parquet'
                LIMIT 5000
                """
                conn.execute(weekly_query) 
                results["stg_weekly_enhanced"] = "Created from parquet"
            
            # Create schedules view
            if not self.table_exists("stg_schedules_enhanced"):
                schedules_query = """
                CREATE VIEW IF NOT EXISTS stg_schedules_enhanced AS
                SELECT 
                    game_id,
                    season,
                    week,
                    home_team,
                    away_team,
                    home_score,
                    away_score,
                    CASE 
                        WHEN home_score > away_score THEN home_team
                        WHEN away_score > home_score THEN away_team
                        ELSE null
                    END as winning_team,
                    ABS(home_score - away_score) as score_differential,
                    home_score + away_score as total_points,
                    CASE
                        WHEN ABS(home_score - away_score) <= 3 THEN 'Very Close (0-3)'
                        WHEN ABS(home_score - away_score) <= 7 THEN 'Close (4-7)'
                        WHEN ABS(home_score - away_score) <= 14 THEN 'Moderate (8-14)'
                        ELSE 'Blowout (15+)'
                    END as game_competitiveness,
                    CASE
                        WHEN game_type = 'REG' THEN 'Regular Season'
                        WHEN game_type = 'WC' THEN 'Wild Card'
                        WHEN game_type = 'DIV' THEN 'Divisional Round'
                        WHEN game_type = 'CON' THEN 'Conference Championship'
                        WHEN game_type = 'SB' THEN 'Super Bowl'
                        ELSE game_type
                    END as game_type_display,
                    current_timestamp as dbt_loaded_at,
                    'fallback_view' as dbt_model
                FROM 'data/schedules/*/etl_date=*/data.parquet'
                """
                conn.execute(schedules_query)
                results["stg_schedules_enhanced"] = "Created from parquet"
                
        except Exception as e:
            results["error"] = f"Failed to create views: {e}"
        
        return results


@st.cache_data
def get_dbt_connection():
    """Get cached DuckDB connection for dbt queries."""
    return DbtDuckDBConnection()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def query_staging_table(table_name: str, limit: int = 1000) -> pd.DataFrame:
    """Query a staging table with caching."""
    conn = get_dbt_connection()
    
    # Ensure staging views exist
    conn.create_staging_tables_from_parquet()
    
    query = f"SELECT * FROM {table_name} LIMIT {limit}"
    return conn.execute_query(query)


@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_staging_table_summary() -> Dict[str, Any]:
    """Get summary of all staging tables."""
    conn = get_dbt_connection()
    
    # Ensure staging views exist
    view_results = conn.create_staging_tables_from_parquet()
    
    summary = {
        "available_tables": conn.get_available_staging_tables(),
        "view_creation_results": view_results,
        "last_updated": datetime.now().isoformat()
    }
    
    # Get table info for each available table
    table_info = {}
    for table in summary["available_tables"]:
        table_info[table] = conn.get_table_info(table)
    
    summary["table_info"] = table_info
    return summary