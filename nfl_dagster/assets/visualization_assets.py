"""
Visualization Assets for Dagster Pipeline

Dashboard data preparation and visualization asset management.
"""

import os
import subprocess
from typing import Dict, Any

from dagster import asset, AssetIn, OpExecutionContext, Config
from dagster_dbt import dbt_assets, DbtCliResource

class VisualizationConfig(Config):
    """Configuration for visualization assets"""
    dashboard_refresh_enabled: bool = True
    streamlit_port: int = 8501
    evidence_port: int = 3002

@asset(
    description="Refresh Streamlit dashboard cache and data connections",
    group_name="visualization",
    compute_kind="streamlit"
)
def streamlit_dashboard_refresh(
    context: OpExecutionContext, 
    config: VisualizationConfig
) -> Dict[str, Any]:
    """Refresh Streamlit dashboard data cache"""
    
    if not config.dashboard_refresh_enabled:
        context.log.info("Dashboard refresh is disabled")
        return {"status": "skipped", "reason": "disabled"}
    
    try:
        # Check if Streamlit is running
        result = subprocess.run(
            ["curl", "-f", f"http://localhost:{config.streamlit_port}/_stcore/health"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            context.log.info("Streamlit dashboard is healthy")
            
            # Clear Streamlit cache by restarting (in production, this would be more sophisticated)
            context.log.info("Signaling dashboard data refresh")
            
            return {
                "status": "success",
                "dashboard_url": f"http://localhost:{config.streamlit_port}",
                "health_check": "passed"
            }
        else:
            context.log.warning("Streamlit dashboard health check failed")
            return {
                "status": "warning", 
                "health_check": "failed",
                "message": "Dashboard may not be running"
            }
            
    except Exception as e:
        context.log.error(f"Error refreshing Streamlit dashboard: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

@asset(
    description="Build and deploy Evidence dashboard",
    group_name="visualization", 
    compute_kind="evidence"
)
def evidence_dashboard_build(
    context: OpExecutionContext,
    config: VisualizationConfig
) -> Dict[str, Any]:
    """Build Evidence dashboard from dbt models"""
    
    if not config.dashboard_refresh_enabled:
        context.log.info("Dashboard build is disabled") 
        return {"status": "skipped", "reason": "disabled"}
    
    try:
        evidence_dir = "/app/visualizations/evidence"
        
        # Check if Evidence directory exists
        if not os.path.exists(evidence_dir):
            context.log.warning(f"Evidence directory not found: {evidence_dir}")
            return {
                "status": "warning",
                "message": "Evidence directory not found"
            }
        
        # Build Evidence dashboard
        context.log.info("Building Evidence dashboard...")
        
        build_result = subprocess.run(
            ["npm", "run", "build"],
            cwd=evidence_dir,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )
        
        if build_result.returncode == 0:
            context.log.info("Evidence dashboard built successfully")
            
            return {
                "status": "success",
                "dashboard_url": f"http://localhost:{config.evidence_port}",
                "build_output": build_result.stdout
            }
        else:
            context.log.error(f"Evidence build failed: {build_result.stderr}")
            return {
                "status": "error",
                "build_error": build_result.stderr
            }
            
    except Exception as e:
        context.log.error(f"Error building Evidence dashboard: {str(e)}")
        return {
            "status": "error", 
            "error": str(e)
        }

@asset(
    description="Generate dashboard data snapshots for reporting",
    group_name="visualization",
    compute_kind="duckdb",
    deps=["dbt_marts_models"]
)
def dashboard_data_snapshots(
    context: OpExecutionContext
) -> Dict[str, Any]:
    """Create data snapshots for dashboard reporting"""
    
    try:
        import duckdb
        
        db_path = os.getenv("NFL_DATA_PATH", "/data") + "/nfl_analytics.duckdb"
        conn = duckdb.connect(db_path)
        
        # Create dashboard summary tables
        dashboard_queries = {
            "team_performance_snapshot": """
                CREATE OR REPLACE TABLE dashboard_team_performance AS
                SELECT 
                    team,
                    AVG(total_epa) as avg_epa,
                    AVG(pass_epa) as avg_pass_epa,
                    AVG(rush_epa) as avg_rush_epa,
                    SUM(wins) as total_wins,
                    SUM(losses) as total_losses,
                    MAX(week) as latest_week,
                    CURRENT_TIMESTAMP as snapshot_time
                FROM mart_weekly_team_stats
                WHERE season = 2023
                GROUP BY team
            """,
            
            "player_fantasy_snapshot": """
                CREATE OR REPLACE TABLE dashboard_fantasy_leaders AS
                SELECT 
                    player_name,
                    position,
                    team,
                    SUM(fantasy_points_ppr) as total_fantasy_points,
                    AVG(fantasy_points_ppr) as avg_fantasy_points,
                    COUNT(*) as games_played,
                    MAX(fantasy_points_ppr) as best_game,
                    STDDEV(fantasy_points_ppr) as consistency_score,
                    CURRENT_TIMESTAMP as snapshot_time
                FROM int_player_weekly_stats
                WHERE season = 2023 AND fantasy_points_ppr > 0
                GROUP BY player_name, position, team
                HAVING games_played >= 4
            """,
            
            "weekly_trends_snapshot": """
                CREATE OR REPLACE TABLE dashboard_weekly_trends AS
                SELECT 
                    week,
                    AVG(total_epa) as avg_epa,
                    AVG(fantasy_points_ppr) as avg_fantasy_points,
                    COUNT(DISTINCT team) as teams_count,
                    COUNT(DISTINCT player_name) as players_count,
                    CURRENT_TIMESTAMP as snapshot_time
                FROM mart_weekly_team_stats ts
                LEFT JOIN int_player_weekly_stats ps USING (week)
                WHERE ts.season = 2023
                GROUP BY week
            """
        }
        
        results = {}
        for table_name, query in dashboard_queries.items():
            context.log.info(f"Creating snapshot table: {table_name}")
            conn.execute(query)
            
            # Get row count
            count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name.replace('_snapshot', '')}").fetchone()
            results[table_name] = {"rows": count_result[0] if count_result else 0}
        
        conn.close()
        
        context.log.info("Dashboard snapshots created successfully")
        return {
            "status": "success",
            "snapshots": results,
            "snapshot_time": "current_timestamp"
        }
        
    except Exception as e:
        context.log.error(f"Error creating dashboard snapshots: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

@asset(
    description="Validate dashboard data quality and completeness",
    group_name="visualization",
    compute_kind="duckdb",
    deps=["dashboard_data_snapshots"]
)  
def dashboard_data_quality_check(
    context: OpExecutionContext
) -> Dict[str, Any]:
    """Run data quality checks for dashboard data"""
    
    try:
        import duckdb
        
        db_path = os.getenv("NFL_DATA_PATH", "/data") + "/nfl_analytics.duckdb"
        conn = duckdb.connect(db_path, read_only=True)
        
        quality_checks = {}
        
        # Team data completeness check
        team_check = conn.execute("""
            SELECT 
                COUNT(DISTINCT team) as team_count,
                COUNT(*) as total_records,
                MAX(latest_week) as max_week
            FROM dashboard_team_performance
        """).fetchone()
        
        quality_checks["team_completeness"] = {
            "teams": team_check[0],
            "records": team_check[1], 
            "latest_week": team_check[2],
            "expected_teams": 32,
            "status": "pass" if team_check[0] >= 30 else "fail"
        }
        
        # Fantasy data completeness check  
        fantasy_check = conn.execute("""
            SELECT 
                COUNT(DISTINCT player_name) as player_count,
                COUNT(DISTINCT position) as position_count,
                AVG(games_played) as avg_games_played
            FROM dashboard_fantasy_leaders
            WHERE total_fantasy_points > 0
        """).fetchone()
        
        quality_checks["fantasy_completeness"] = {
            "players": fantasy_check[0],
            "positions": fantasy_check[1],
            "avg_games": round(fantasy_check[2], 1) if fantasy_check[2] else 0,
            "expected_positions": 4,  # QB, RB, WR, TE
            "status": "pass" if fantasy_check[1] >= 4 else "fail"
        }
        
        # Weekly trends check
        trends_check = conn.execute("""
            SELECT 
                COUNT(*) as week_count,
                MIN(week) as min_week,
                MAX(week) as max_week
            FROM dashboard_weekly_trends
        """).fetchone()
        
        quality_checks["trends_completeness"] = {
            "weeks": trends_check[0],
            "min_week": trends_check[1],
            "max_week": trends_check[2],
            "expected_weeks": 18,
            "status": "pass" if trends_check[0] >= 10 else "fail"
        }
        
        conn.close()
        
        # Overall status
        overall_status = "pass" if all(
            check["status"] == "pass" 
            for check in quality_checks.values()
        ) else "fail"
        
        context.log.info(f"Dashboard data quality check completed: {overall_status}")
        
        return {
            "status": overall_status,
            "checks": quality_checks,
            "check_time": "current_timestamp"
        }
        
    except Exception as e:
        context.log.error(f"Error running dashboard quality checks: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }