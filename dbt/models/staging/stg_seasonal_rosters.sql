{{ config(materialized='view', tags=['staging', 'medium']) }}

WITH raw_seasonal_rosters AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'seasonal_rosters') }}
),

cleaned_seasonal_rosters AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    position,
    jersey_number,
    
    -- Team and season
    team,
    season,
    
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
    
    -- Season stats
    games_played,
    games_started,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_seasonal_rosters
  WHERE player_id IS NOT NULL
    AND team IS NOT NULL
    AND season IS NOT NULL
)

SELECT * FROM cleaned_seasonal_rosters