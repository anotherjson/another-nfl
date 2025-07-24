{{ config(materialized='view', tags=['staging', 'critical']) }}

WITH raw_schedules AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'schedules') }}
),

cleaned_schedules AS (
  SELECT
    -- Game identifiers
    game_id,
    season,
    game_type,
    week,
    
    -- Game timing
    gameday,
    weekday,
    gametime,
    
    -- Teams
    home_team,
    away_team,
    
    -- Results
    home_score,
    away_score,
    result,
    total,
    overtime,
    
    -- Betting lines
    spread_line,
    total_line,
    
    -- Stadium and conditions
    stadium,
    location,
    surface,
    temp,
    wind,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_schedules
  WHERE game_id IS NOT NULL
    AND season IS NOT NULL
)

SELECT * FROM cleaned_schedules