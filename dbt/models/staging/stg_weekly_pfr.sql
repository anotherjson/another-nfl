{{ config(materialized='view', tags=['staging', 'low']) }}

WITH raw_weekly_pfr AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'weekly_pfr') }}
),

cleaned_weekly_pfr AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    position,
    
    -- Team and game info
    team,
    opponent,
    season,
    week,
    
    -- Passing stats (PFR specific)
    passing_cmp,
    passing_att,
    passing_yds,
    passing_td,
    passing_int,
    passing_rate,
    
    -- Rushing stats (PFR specific)
    rushing_att,
    rushing_yds,
    rushing_td,
    rushing_lng,
    
    -- Receiving stats (PFR specific)
    receiving_rec,
    receiving_yds,
    receiving_td,
    receiving_lng,
    receiving_tgt,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_weekly_pfr
  WHERE player_id IS NOT NULL
    AND season IS NOT NULL
    AND week IS NOT NULL
)

SELECT * FROM cleaned_weekly_pfr