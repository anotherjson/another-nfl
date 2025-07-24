{{ config(materialized='view', tags=['staging', 'medium']) }}

WITH raw_weekly_rosters AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'weekly_rosters') }}
),

cleaned_weekly_rosters AS (
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
    
    -- Player details
    status,
    depth_chart_position,
    rookie_year,
    years_exp,
    
    -- Physical attributes
    height,
    weight,
    birth_date,
    college,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_weekly_rosters
  WHERE player_id IS NOT NULL
    AND team IS NOT NULL
    AND season IS NOT NULL
    AND week IS NOT NULL
)

SELECT * FROM cleaned_weekly_rosters