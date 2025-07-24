{{ config(materialized='view', tags=['staging', 'medium']) }}

WITH raw_ngs_data AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'ngs_data') }}
),

cleaned_ngs_data AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    player_display_name,
    position,
    
    -- Team and season info
    team,
    season,
    week,
    
    -- Next Gen Stats metrics (varies by position)
    -- Passing metrics
    avg_time_to_throw,
    avg_completed_air_yards,
    avg_intended_air_yards,
    avg_air_yards_differential,
    aggressiveness,
    max_completed_air_distance,
    avg_air_yards_to_sticks,
    passer_rating,
    
    -- Rushing metrics
    efficiency,
    percent_attempts_gte_eight_defenders,
    avg_rush_yards,
    
    -- Receiving metrics
    avg_cushion,
    avg_separation,
    avg_target_separation,
    share_of_air_yards,
    target_share,
    
    -- Common metrics
    attempts,
    completions,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_ngs_data
  WHERE player_id IS NOT NULL
    AND season IS NOT NULL
)

SELECT * FROM cleaned_ngs_data