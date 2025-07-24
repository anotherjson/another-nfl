{{ config(materialized='view', tags=['staging', 'low']) }}

WITH raw_combine AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'combine') }}
),

cleaned_combine AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    position,
    
    -- Draft and combine year
    season as combine_year,
    draft_year,
    draft_round,
    draft_pick,
    
    -- Physical measurements
    height,
    weight,
    hand_size,
    arm_length,
    
    -- Combine performance
    forty_yard_dash,
    bench_press,
    vertical_jump,
    broad_jump,
    three_cone_drill,
    twenty_yard_shuttle,
    
    -- College info
    college,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_combine
  WHERE player_id IS NOT NULL
)

SELECT * FROM cleaned_combine