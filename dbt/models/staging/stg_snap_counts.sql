{{ config(materialized='view', tags=['staging', 'medium']) }}

WITH raw_snap_counts AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'snap_counts') }}
),

cleaned_snap_counts AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    position,
    
    -- Team and game info
    team,
    opponent,
    season,
    week,
    
    -- Snap count data
    offense_snaps,
    offense_pct,
    defense_snaps,
    defense_pct,
    st_snaps,
    st_pct,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_snap_counts
  WHERE player_id IS NOT NULL
    AND season IS NOT NULL
    AND week IS NOT NULL
)

SELECT * FROM cleaned_snap_counts