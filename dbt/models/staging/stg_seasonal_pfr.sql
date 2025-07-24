{{ config(materialized='view', tags=['staging', 'low']) }}

WITH raw_seasonal_pfr AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'seasonal_pfr') }}
),

cleaned_seasonal_pfr AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    position,
    
    -- Team and season
    team,
    season,
    
    -- Games played
    games,
    games_started,
    
    -- Passing stats (PFR seasonal)
    passing_cmp,
    passing_att,
    passing_yds,
    passing_td,
    passing_int,
    passing_rate,
    passing_yds_per_att,
    
    -- Rushing stats (PFR seasonal)
    rushing_att,
    rushing_yds,
    rushing_td,
    rushing_yds_per_att,
    
    -- Receiving stats (PFR seasonal)
    receiving_rec,
    receiving_yds,
    receiving_td,
    receiving_yds_per_rec,
    receiving_tgt,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_seasonal_pfr
  WHERE player_id IS NOT NULL
    AND season IS NOT NULL
)

SELECT * FROM cleaned_seasonal_pfr