{{ config(materialized='view', tags=['staging', 'critical']) }}

WITH raw_weekly AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'weekly') }}
),

cleaned_weekly AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    player_display_name,
    position,
    
    -- Team and game info
    team,
    opponent_team,
    season,
    week,
    
    -- Passing stats
    completions,
    attempts,
    passing_yards,
    passing_tds,
    interceptions,
    
    -- Rushing stats
    carries,
    rushing_yards,
    rushing_tds,
    
    -- Receiving stats
    targets,
    receptions,
    receiving_yards,
    receiving_tds,
    
    -- Fantasy points
    fantasy_points,
    fantasy_points_ppr,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_weekly
  WHERE player_id IS NOT NULL
    AND season IS NOT NULL
    AND week IS NOT NULL
)

SELECT * FROM cleaned_weekly