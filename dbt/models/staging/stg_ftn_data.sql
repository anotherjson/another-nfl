{{ config(materialized='view', tags=['staging', 'low']) }}

WITH raw_ftn_data AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'ftn_data') }}
),

cleaned_ftn_data AS (
  SELECT
    -- Team and timing
    team,
    season,
    week,
    
    -- Opponent and matchup info
    opponent,
    
    -- Fantasy points allowed by position
    qb_fpts,
    rb_fpts,
    wr_fpts,
    te_fpts,
    k_fpts,
    dst_fpts,
    
    -- Additional metrics
    games,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_ftn_data
  WHERE team IS NOT NULL
    AND season IS NOT NULL
)

SELECT * FROM cleaned_ftn_data