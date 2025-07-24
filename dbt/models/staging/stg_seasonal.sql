{{ config(materialized='view', tags=['staging', 'high']) }}

WITH raw_seasonal AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'seasonal') }}
),

cleaned_seasonal AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    player_display_name,
    position,
    position_group,
    
    -- Team and season info
    team,
    season,
    
    -- Games played
    games,
    
    -- Passing stats
    completions,
    attempts,
    passing_yards,
    passing_tds,
    interceptions,
    sacks,
    sack_yards,
    sack_fumbles,
    sack_fumbles_lost,
    passing_air_yards,
    passing_yards_after_catch,
    passing_first_downs,
    passing_epa,
    
    -- Rushing stats
    carries,
    rushing_yards,
    rushing_tds,
    rushing_fumbles,
    rushing_fumbles_lost,
    rushing_first_downs,
    rushing_epa,
    
    -- Receiving stats
    targets,
    receptions,
    receiving_yards,
    receiving_tds,
    receiving_fumbles,
    receiving_fumbles_lost,
    receiving_air_yards,
    receiving_yards_after_catch,
    receiving_first_downs,
    receiving_epa,
    
    -- Fantasy points
    fantasy_points,
    fantasy_points_ppr,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_seasonal
  WHERE player_id IS NOT NULL
    AND season IS NOT NULL
)

SELECT * FROM cleaned_seasonal