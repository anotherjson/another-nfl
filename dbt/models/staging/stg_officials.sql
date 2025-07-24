{{ config(materialized='view', tags=['staging', 'low']) }}

WITH raw_officials AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'officials') }}
),

cleaned_officials AS (
  SELECT
    -- Game identifiers
    game_id,
    season,
    week,
    
    -- Team info
    home_team,
    away_team,
    
    -- Official details
    referee,
    umpire,
    down_judge,
    line_judge,
    field_judge,
    side_judge,
    back_judge,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_officials
  WHERE game_id IS NOT NULL
    AND season IS NOT NULL
)

SELECT * FROM cleaned_officials