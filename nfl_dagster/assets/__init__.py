# Legacy assets (kept for compatibility)
from .raw_data_assets import *
from .dbt_assets import *
from .ducklake_assets import *
from .visualization_assets import *

# New unified Dagster-managed assets
from .nfl_raw_data_assets import *
from .nfl_staging_assets import *

__all__ = [
    # Legacy assets
    "pbp_data",
    "weekly_data", 
    "team_desc_data",
    "schedules_data",
    "dbt_staging_models",
    "dbt_intermediate_models", 
    "dbt_marts_models",
    "streamlit_dashboard_refresh",
    "evidence_dashboard_build",
    "dashboard_data_snapshots",
    "dashboard_data_quality_check",
    
    # New comprehensive NFL raw data assets
    "nfl_critical_raw_data",
    "nfl_high_priority_raw_data", 
    "nfl_medium_priority_raw_data",
    "nfl_low_priority_raw_data",
    "nfl_raw_data_health_check",
    
    # New dbt staging assets
    "dbt_critical_staging_models",
    "dbt_high_priority_staging_models",
    "dbt_medium_priority_staging_models",
    "dbt_intermediate_models",
    "dbt_staging_validation",
]