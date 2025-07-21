{{ config(
    materialized='table',
    description='Game results and matchup analysis for analytics and ML features'
) }}

with game_data as (
    select
        s.game_id,
        s.season,
        s.week,
        s.game_type,
        s.game_date,
        s.home_team,
        s.away_team,
        s.home_score,
        s.away_score,
        s.total as total_points,
        s.overtime,
        s.stadium,
        s.surface,
        s.temp,
        s.wind,
        s.spread_line,
        s.total_line,
        
        -- Team information
        ht.team_name as home_team_name,
        ht.conference as home_conference,
        ht.division as home_division,
        at.team_name as away_team_name,
        at.conference as away_conference,
        at.division as away_division,
        
        -- Game outcome
        case 
            when s.home_score > s.away_score then s.home_team
            when s.away_score > s.home_score then s.away_team
            else null
        end as winning_team,
        case
            when s.home_score > s.away_score then s.away_team
            when s.away_score > s.home_score then s.home_team
            else null
        end as losing_team,
        
        abs(s.home_score - s.away_score) as point_differential,
        
        -- Betting outcomes
        case 
            when s.spread_line is not null then s.home_score + s.spread_line - s.away_score 
            else null 
        end as spread_result,
        case 
            when s.total_line is not null then (s.home_score + s.away_score) - s.total_line
            else null 
        end as total_result
        
    from {{ ref('stg_schedules') }} s
    left join {{ ref('stg_team_desc') }} ht on s.home_team = ht.team_id
    left join {{ ref('stg_team_desc') }} at on s.away_team = at.team_id
    where s.home_score is not null 
      and s.away_score is not null
),

with_context as (
    select
        *,
        -- Division/conference matchups
        case when home_division = away_division then 1 else 0 end as division_game,
        case when home_conference = away_conference then 1 else 0 end as conference_game,
        
        -- Game characteristics
        case when point_differential <= 3 then 1 else 0 end as close_game,
        case when point_differential >= 14 then 1 else 0 end as blowout,
        case when total_points > 45 then 1 else 0 end as high_scoring,
        case when total_points < 35 then 1 else 0 end as low_scoring,
        
        -- Betting results
        case when spread_result > 0 then 1 else 0 end as home_covered_spread,
        case when total_result > 0 then 1 else 0 end as over_hit,
        
        -- Season context
        case 
            when week <= 4 then 'Early'
            when week <= 12 then 'Mid' 
            else 'Late'
        end as season_phase
        
    from game_data
)

select
    game_id,
    season,
    week,
    game_type,
    game_date,
    season_phase,
    
    -- Teams
    home_team,
    home_team_name,
    home_conference,
    home_division,
    away_team,
    away_team_name,
    away_conference,
    away_division,
    
    -- Scores
    home_score,
    away_score,
    total_points,
    point_differential,
    winning_team,
    losing_team,
    overtime,
    
    -- Game characteristics
    division_game,
    conference_game,
    close_game,
    blowout,
    high_scoring,
    low_scoring,
    
    -- Conditions
    stadium,
    surface,
    temp,
    wind,
    
    -- Betting
    spread_line,
    total_line,
    spread_result,
    total_result,
    home_covered_spread,
    over_hit,
    
    -- Data quality
    current_timestamp() as dbt_loaded_at
    
from with_context
where season >= 2020  -- Focus on recent seasons