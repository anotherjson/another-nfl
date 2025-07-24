{{ config(materialized='view', tags=['staging', 'critical']) }}

WITH raw_pbp AS (
  SELECT *,
    -- Add staging metadata
    CURRENT_TIMESTAMP as dbt_loaded_at,
    '{{ env_var("DBT_RUN_ID", "manual") }}' as dbt_run_id
  FROM {{ source('nfl_raw', 'pbp') }}
),

cleaned_pbp as (
    select
        -- Game identifiers
        game_id,
        play_id,
        season,
        week,
        game_date,
        
        -- Team information
        home_team,
        away_team,
        posteam as possession_team,
        defteam as defense_team,
        
        -- Play timing
        quarter_seconds_remaining,
        half_seconds_remaining,
        game_seconds_remaining,
        qtr as quarter,
        
        -- Field position
        yardline_100,
        down,
        ydstogo as yards_to_go,
        
        -- Play type and description
        play_type,
        pass,
        rush,
        "desc" as play_description,
        
        -- Scoring
        touchdown,
        field_goal_attempt,
        extra_point_attempt,
        safety,
        
        -- Advanced metrics
        epa,
        wp as win_probability,
        wpa as win_probability_added,
        
        -- Player information
        passer_id,
        passer_player_name as passer_name,
        rusher_id,
        rusher_player_name as rusher_name,
        receiver_id,
        receiver_player_name as receiver_name,
        
        -- Outcomes
        yards_gained,
        passing_yards,
        rushing_yards,
        receiving_yards,
        
        -- Data quality fields from DuckLake
        dbt_loaded_at,
        dbt_run_id
        
    from raw_pbp
    where game_id is not null
      and play_id is not null
)

select * from cleaned_pbp