"""
Demo script showcasing the new dbt model CLI functionality.

This script demonstrates the complete workflow:
1. Materializing dbt staging models via Dagster
2. Querying models through DuckLake
3. Time travel queries
4. Custom SQL analytics

Usage:
    uv run python scripts/demo_dbt_models.py
"""

import subprocess
import sys
from pathlib import Path

def run_command(cmd: list, description: str):
    """Run a CLI command and display results."""
    print(f"\n{'='*60}")
    print(f"🔄 {description}")
    print(f"Command: {' '.join(cmd)}")
    print('='*60)
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("✅ SUCCESS")
        if result.stdout:
            print(result.stdout)
    else:
        print("❌ FAILED")
        if result.stderr:
            print(f"Error: {result.stderr}")
    
    print()
    return result.returncode == 0

def main():
    """Run the demo workflow."""
    print("🏈 NFL dbt Models CLI Demo")
    print("This demo showcases the new dbt model functionality")
    print()
    
    # Base command
    base_cmd = ["uv", "run", "python", "-m", "src.cli", "models"]
    
    # 1. List available models
    if not run_command(
        base_cmd + ["list"],
        "Listing available dbt staging models"
    ):
        print("⚠️  Could not list models. Make sure the environment is set up correctly.")
        return
    
    # 2. Show catalog status
    run_command(
        base_cmd + ["catalog"],
        "Showing DuckLake catalog status"
    )
    
    # 3. Materialize models (if needed)
    print("Do you want to materialize the dbt staging models? (y/n): ", end="")
    response = input().strip().lower()
    
    if response == 'y':
        if run_command(
            base_cmd + ["materialize", "--verbose"],
            "Materializing dbt staging models via Dagster"
        ):
            print("✅ Models materialized successfully!")
            
            # Show updated catalog
            run_command(
                base_cmd + ["catalog"],
                "Updated DuckLake catalog after materialization"
            )
        else:
            print("⚠️  Materialization failed. Continuing with existing data...")
    
    # 4. Query team descriptions (most stable dataset)
    run_command(
        base_cmd + ["query", "stg_team_desc", "--limit", "5", "--show-schema"],
        "Querying team descriptions with schema info"
    )
    
    # 5. Show version history for team descriptions
    run_command(
        base_cmd + ["versions", "nfl_raw.team_desc"],
        "Showing version history for team descriptions"
    )
    
    # 6. Custom SQL query - count teams by division
    sql_query = """
    SELECT 
        team_division,
        COUNT(*) as team_count
    FROM 'data/team_desc/etl_date=*/data.parquet'
    WHERE team_division IS NOT NULL
    GROUP BY team_division
    ORDER BY team_count DESC
    """
    
    run_command(
        base_cmd + ["sql", sql_query],
        "Custom SQL: Teams by division"
    )
    
    # 7. If schedule data exists, show games analysis
    run_command(
        base_cmd + ["query", "stg_schedules", "--limit", "3"],
        "Querying recent schedule data (if available)"
    )
    
    # 8. Another custom SQL - show data freshness
    freshness_query = """
    SELECT 
        'team_desc' as dataset,
        COUNT(*) as row_count,
        MAX(dbt_loaded_at) as latest_load
    FROM 'data/team_desc/etl_date=*/data.parquet'
    WHERE dbt_loaded_at IS NOT NULL
    """
    
    run_command(
        base_cmd + ["sql", freshness_query],
        "Custom SQL: Data freshness check"
    )
    
    print("🎉 Demo completed!")
    print()
    print("Key takeaways from this demo:")
    print("• Use 'models list' to see available dbt staging models")
    print("• Use 'models materialize' to refresh models via Dagster")
    print("• Use 'models query <model>' to query transformed data")
    print("• Use 'models catalog' to see DuckLake table versions")
    print("• Use 'models sql <query>' for custom analytics")
    print("• Time travel with '--as-of-date' for historical analysis")
    print()
    print("For more examples, see:")
    print("• README.md - Complete usage documentation")
    print("• CLAUDE.md - Development commands reference")

if __name__ == "__main__":
    main()