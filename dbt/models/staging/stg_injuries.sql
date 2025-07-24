{{ config(materialized='view', tags=['staging', 'medium']) }}

WITH raw_injuries AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'injuries') }}
),

cleaned_injuries AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    position,
    
    -- Team and season info
    team,
    season,
    week,
    
    -- Injury details
    report_status,
    report_primary_injury,
    report_secondary_injury,
    report_date,
    
    -- Practice participation
    practice_primary_injury,
    practice_secondary_injury,
    date_modified,
    
    -- Game status
    game_status,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_injuries
  WHERE player_id IS NOT NULL
    AND season IS NOT NULL
)

SELECT * FROM cleaned_injuries