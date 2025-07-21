{{ config(
    materialized='table',
    description='Weekly team performance metrics for dashboard consumption'
) }}

select
    season,
    week,
    team_id,
    team_name,
    conference,
    division,
    
    -- Game results
    wins,
    losses,
    ties,
    case 
        when wins + losses + ties > 0 
        then round(wins::float / (wins + losses + ties), 3)
        else 0 
    end as win_percentage,
    
    -- Offensive metrics
    total_plays,
    total_yards,
    yards_per_play,
    passing_yards,
    rushing_yards,
    yards_per_pass,
    yards_per_rush,
    pass_rate,
    run_rate,
    touchdowns,
    
    -- Advanced metrics
    avg_epa,
    total_epa,
    
    -- Season context
    rank() over (partition by season, week, conference order by total_yards desc) as conference_yards_rank,
    rank() over (partition by season, week, division order by total_yards desc) as division_yards_rank,
    rank() over (partition by season, week order by total_yards desc) as league_yards_rank,
    
    rank() over (partition by season, week, conference order by avg_epa desc) as conference_epa_rank,
    rank() over (partition by season, week, division order by avg_epa desc) as division_epa_rank,
    rank() over (partition by season, week order by avg_epa desc) as league_epa_rank,
    
    -- Data quality
    games_played,
    dbt_loaded_at

from {{ ref('int_team_performance') }}
where team_id is not null 
  and season >= 2020  -- Focus on recent data for analytics