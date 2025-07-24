{{ config(materialized='view', tags=['staging', 'critical']) }}

WITH raw_team_desc AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'team_desc') }}
),

cleaned_team_desc AS (
  SELECT
    -- Team identifiers
    team_abbr,
    team_id,
    team_name,
    team_nick,
    team_color,
    team_color2,
    team_logo_espn,
    team_logo_wikipedia,
    
    -- Conference and division
    team_conference,
    team_division,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_team_desc
  WHERE team_abbr IS NOT NULL
)

SELECT * FROM cleaned_team_desc