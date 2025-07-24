{{ config(materialized='view', tags=['staging', 'medium']) }}

WITH raw_depth_charts AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'depth_charts') }}
),

cleaned_depth_charts AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    position,
    jersey_number,
    
    -- Team and timing
    team,
    season,
    week,
    
    -- Depth chart position
    depth_team,
    formation,
    gsis_id,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_depth_charts
  WHERE player_id IS NOT NULL
    AND team IS NOT NULL
    AND season IS NOT NULL
)

SELECT * FROM cleaned_depth_charts