{{ config(materialized='view', tags=['staging', 'medium']) }}

WITH raw_qbr AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'qbr') }}
),

cleaned_qbr AS (
  SELECT
    -- Player identifiers
    player_id,
    player_name,
    position,
    
    -- Team and game info
    team,
    season,
    week,
    game_id,
    
    -- QBR metrics
    qbr_total,
    pts_added,
    qb_plays,
    
    -- Passing QBR components
    pass_qbr,
    pass_att,
    pass_yds,
    pass_td,
    pass_int,
    pass_epa,
    
    -- Rushing QBR components
    rush_qbr,
    rush_att,
    rush_yds,
    rush_td,
    rush_epa,
    
    -- Penalty impact
    pen_qbr,
    
    -- Metadata
    dbt_loaded_at,
    dbt_run_id
    
  FROM raw_qbr
  WHERE player_id IS NOT NULL
    AND season IS NOT NULL
    AND position = 'QB'
)

SELECT * FROM cleaned_qbr