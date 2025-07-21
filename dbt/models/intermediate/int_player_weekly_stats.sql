{{ config(
    materialized='table',
    description='Weekly player statistics with position-specific metrics and rankings'
) }}

with player_stats as (
    select
        player_id,
        player_name,
        position,
        position_group,
        team,
        season,
        week,
        
        -- Receiving stats
        targets,
        receptions,
        receiving_yards,
        receiving_tds,
        receiving_fumbles,
        receiving_first_downs,
        
        -- Rushing stats
        carries,
        rushing_yards,
        rushing_tds,
        rushing_fumbles,
        rushing_first_downs,
        
        -- Passing stats
        completions,
        attempts,
        passing_yards,
        passing_tds,
        interceptions,
        sacks,
        sack_yards,
        
        -- Fantasy points
        fantasy_points,
        fantasy_points_ppr
        
    from {{ ref('stg_weekly') }}
),

position_rankings as (
    select
        *,
        -- Weekly position rankings
        row_number() over (partition by season, week, position_group order by fantasy_points_ppr desc) as position_rank_week,
        row_number() over (partition by season, week, position order by fantasy_points_ppr desc) as specific_position_rank_week,
        
        -- Season-to-date rankings
        row_number() over (partition by season, position_group order by sum(fantasy_points_ppr) over (partition by player_id, season order by week) desc) as position_rank_std,
        
        -- Calculate rates and efficiency
        case when attempts > 0 then round(completions::float / attempts, 3) else 0 end as completion_percentage,
        case when targets > 0 then round(receptions::float / targets, 3) else 0 end as catch_rate,
        case when carries > 0 then round(rushing_yards::float / carries, 2) else 0 end as yards_per_carry,
        case when receptions > 0 then round(receiving_yards::float / receptions, 2) else 0 end as yards_per_reception,
        case when attempts > 0 then round(passing_yards::float / attempts, 2) else 0 end as yards_per_attempt
        
    from player_stats
)

select
    player_id,
    player_name,
    position,
    position_group,
    team,
    season,
    week,
    
    -- Basic stats
    targets,
    receptions,
    receiving_yards,
    receiving_tds,
    carries,
    rushing_yards,
    rushing_tds,
    completions,
    attempts,
    passing_yards,
    passing_tds,
    interceptions,
    
    -- Calculated metrics
    completion_percentage,
    catch_rate,
    yards_per_carry,
    yards_per_reception,
    yards_per_attempt,
    
    -- Fantasy
    fantasy_points,
    fantasy_points_ppr,
    
    -- Rankings
    position_rank_week,
    specific_position_rank_week,
    position_rank_std,
    
    -- Totals
    receiving_yards + rushing_yards + passing_yards as total_yards,
    receiving_tds + rushing_tds + passing_tds as total_tds,
    
    -- Data quality
    current_timestamp() as dbt_loaded_at
    
from position_rankings
where player_id is not null