{{ config(materialized='view', tags=['staging', 'high']) }}

WITH raw_players AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'players') }}
),

cleaned_players AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    player_display_name,
    
    -- Basic info
    position,
    position_group,
    jersey_number,
    
    -- Physical attributes
    height,
    weight,
    
    -- Career info
    years_exp,
    team,
    college,
    
    -- Birth info
    birth_date,
    age,
    
    -- Draft info
    draft_year,
    draft_round,
    draft_pick,
    draft_ovr,
    
    -- Status
    status,
    
    -- Headshot
    headshot_url,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_players
  WHERE player_id IS NOT NULL
)

SELECT * FROM cleaned_players