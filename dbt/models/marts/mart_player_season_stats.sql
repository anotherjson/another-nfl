{{ config(
    materialized='table',
    description='Season-aggregated player statistics for fantasy and performance analysis'
) }}

with season_totals as (
    select
        player_id,
        player_name,
        position,
        position_group,
        team,
        season,
        
        -- Game counts
        count(distinct week) as games_played,
        
        -- Receiving totals
        sum(targets) as season_targets,
        sum(receptions) as season_receptions,
        sum(receiving_yards) as season_receiving_yards,
        sum(receiving_tds) as season_receiving_tds,
        
        -- Rushing totals
        sum(carries) as season_carries,
        sum(rushing_yards) as season_rushing_yards,
        sum(rushing_tds) as season_rushing_tds,
        
        -- Passing totals
        sum(completions) as season_completions,
        sum(attempts) as season_attempts,
        sum(passing_yards) as season_passing_yards,
        sum(passing_tds) as season_passing_tds,
        sum(interceptions) as season_interceptions,
        
        -- Fantasy totals
        sum(fantasy_points) as season_fantasy_points,
        sum(fantasy_points_ppr) as season_fantasy_points_ppr,
        
        -- Per-game averages
        round(avg(fantasy_points_ppr), 2) as avg_fantasy_points_ppr,
        round(avg(receiving_yards), 1) as avg_receiving_yards,
        round(avg(rushing_yards), 1) as avg_rushing_yards,
        round(avg(passing_yards), 1) as avg_passing_yards
        
    from {{ ref('int_player_weekly_stats') }}
    group by player_id, player_name, position, position_group, team, season
),

season_rankings as (
    select
        *,
        -- Season rankings by position group
        row_number() over (partition by season, position_group order by season_fantasy_points_ppr desc) as position_group_rank,
        row_number() over (partition by season, position order by season_fantasy_points_ppr desc) as position_rank,
        
        -- Percentile rankings
        percent_rank() over (partition by season, position_group order by season_fantasy_points_ppr) as position_group_percentile,
        
        -- Calculate season rates
        case when season_attempts > 0 then round(season_completions::float / season_attempts, 3) else 0 end as season_completion_percentage,
        case when season_targets > 0 then round(season_receptions::float / season_targets, 3) else 0 end as season_catch_rate,
        case when season_carries > 0 then round(season_rushing_yards::float / season_carries, 2) else 0 end as season_yards_per_carry,
        case when season_receptions > 0 then round(season_receiving_yards::float / season_receptions, 2) else 0 end as season_yards_per_reception
        
    from season_totals
)

select
    player_id,
    player_name,
    position,
    position_group,
    team,
    season,
    games_played,
    
    -- Season totals
    season_targets,
    season_receptions,
    season_receiving_yards,
    season_receiving_tds,
    season_carries,
    season_rushing_yards,
    season_rushing_tds,
    season_completions,
    season_attempts,
    season_passing_yards,
    season_passing_tds,
    season_interceptions,
    
    -- Fantasy metrics
    season_fantasy_points,
    season_fantasy_points_ppr,
    avg_fantasy_points_ppr,
    
    -- Efficiency metrics
    season_completion_percentage,
    season_catch_rate,
    season_yards_per_carry,
    season_yards_per_reception,
    
    -- Rankings
    position_group_rank,
    position_rank,
    round(position_group_percentile, 3) as position_group_percentile,
    
    -- Per-game averages
    avg_receiving_yards,
    avg_rushing_yards,
    avg_passing_yards,
    
    -- Composite stats
    season_receiving_yards + season_rushing_yards + season_passing_yards as total_season_yards,
    season_receiving_tds + season_rushing_tds + season_passing_tds as total_season_tds,
    
    -- Data quality
    current_timestamp() as dbt_loaded_at
    
from season_rankings
where games_played >= 4  -- Only include players with meaningful playing time
  and season >= 2020  -- Focus on recent seasons