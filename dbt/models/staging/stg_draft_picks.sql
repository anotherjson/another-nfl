{{ config(materialized='view', tags=['staging', 'low']) }}

WITH raw_draft_picks AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'draft_picks') }}
),

cleaned_draft_picks AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    position,
    
    -- Draft details
    season as draft_year,
    round,
    pick,
    team,
    
    -- Player info
    age,
    
    -- College info
    college,
    
    -- Physical attributes
    height,
    weight,
    
    -- Career outcomes (if available)
    career_games,
    career_starts,
    career_av,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_draft_picks
  WHERE season IS NOT NULL
    AND team IS NOT NULL
)

SELECT * FROM cleaned_draft_picks