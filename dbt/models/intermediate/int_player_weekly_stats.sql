{{ config(
    materialized='table',
    description='Weekly player statistics with enhanced position-specific metrics and advanced analytics'
) }}

with player_stats as (
    select
        player_id,
        player_name,
        position,
        position_group,
        team,
        opponent_team,
        season,
        week,
        
        -- Receiving stats
        targets,
        receptions,
        receiving_yards,
        receiving_tds,
        receiving_fumbles,
        receiving_fumbles_lost,
        receiving_air_yards,
        receiving_yards_after_catch,
        receiving_first_downs,
        receiving_epa,
        receiving_2pt_conversions,
        
        -- Rushing stats
        carries,
        rushing_yards,
        rushing_tds,
        rushing_fumbles,
        rushing_fumbles_lost,
        rushing_first_downs,
        rushing_epa,
        rushing_2pt_conversions,
        
        -- Passing stats
        completions,
        attempts,
        passing_yards,
        passing_tds,
        interceptions,
        sacks,
        sack_yards,
        sack_fumbles,
        sack_fumbles_lost,
        passing_air_yards,
        passing_yards_after_catch,
        passing_first_downs,
        passing_epa,
        passing_2pt_conversions,
        pacr,
        
        -- Special teams
        special_teams_tds,
        
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
        
        -- Season-to-date rankings and rolling averages  
        sum(fantasy_points_ppr) over (partition by player_id, season order by week) as season_fantasy_points_ppr,
        avg(fantasy_points_ppr) over (partition by player_id, season order by week rows between 3 preceding and current row) as fantasy_points_ppr_4wk_avg,
        
        -- Calculate rates and efficiency metrics
        case when attempts > 0 then round(completions::float / attempts, 3) else 0 end as completion_percentage,
        case when targets > 0 then round(receptions::float / targets, 3) else 0 end as catch_rate,
        case when carries > 0 then round(rushing_yards::float / carries, 2) else 0 end as yards_per_carry,
        case when receptions > 0 then round(receiving_yards::float / receptions, 2) else 0 end as yards_per_reception,
        case when attempts > 0 then round(passing_yards::float / attempts, 2) else 0 end as yards_per_attempt,
        case when targets > 0 then round(receiving_air_yards::float / targets, 2) else 0 end as air_yards_per_target,
        case when receptions > 0 then round(receiving_yards_after_catch::float / receptions, 2) else 0 end as yac_per_reception,
        
        -- Advanced efficiency metrics
        case when receiving_air_yards > 0 then round(receiving_yards::float / receiving_air_yards, 3) else 0 end as receiving_air_yards_share,
        case when passing_air_yards > 0 then round(passing_yards::float / passing_air_yards, 3) else 0 end as passing_air_yards_conversion,
        
        -- EPA metrics
        case when targets > 0 then round(receiving_epa::float / targets, 3) else 0 end as receiving_epa_per_target,
        case when carries > 0 then round(rushing_epa::float / carries, 3) else 0 end as rushing_epa_per_carry,
        case when attempts > 0 then round(passing_epa::float / attempts, 3) else 0 end as passing_epa_per_attempt
        
    from player_stats
),

season_rankings as (
    select
        *,
        row_number() over (partition by season, position_group, week order by season_fantasy_points_ppr desc) as position_rank_std
    from position_rankings
)

select
    player_id,
    player_name,
    position,
    position_group,
    team,
    opponent_team,
    season,
    week,
    
    -- Basic receiving stats
    targets,
    receptions,
    receiving_yards,
    receiving_tds,
    receiving_fumbles,
    receiving_fumbles_lost,
    receiving_air_yards,
    receiving_yards_after_catch,
    receiving_first_downs,
    receiving_2pt_conversions,
    
    -- Basic rushing stats
    carries,
    rushing_yards,
    rushing_tds,
    rushing_fumbles,
    rushing_fumbles_lost,
    rushing_first_downs,
    rushing_2pt_conversions,
    
    -- Basic passing stats
    completions,
    attempts,
    passing_yards,
    passing_tds,
    interceptions,
    sacks,
    sack_yards,
    sack_fumbles,
    sack_fumbles_lost,
    passing_air_yards,
    passing_yards_after_catch,
    passing_first_downs,
    passing_2pt_conversions,
    pacr,
    
    -- Special teams
    special_teams_tds,
    
    -- Calculated efficiency metrics
    completion_percentage,
    catch_rate,
    yards_per_carry,
    yards_per_reception,
    yards_per_attempt,
    air_yards_per_target,
    yac_per_reception,
    receiving_air_yards_share,
    passing_air_yards_conversion,
    
    -- EPA metrics
    receiving_epa,
    rushing_epa,
    passing_epa,
    receiving_epa_per_target,
    rushing_epa_per_carry,
    passing_epa_per_attempt,
    
    -- Fantasy and rankings
    fantasy_points,
    fantasy_points_ppr,
    fantasy_points_ppr_4wk_avg,
    position_rank_week,
    specific_position_rank_week,
    position_rank_std,
    
    -- Totals and derived metrics
    receiving_yards + rushing_yards + passing_yards as total_yards,
    receiving_tds + rushing_tds + passing_tds + special_teams_tds as total_tds,
    receiving_2pt_conversions + rushing_2pt_conversions + passing_2pt_conversions as total_2pt_conversions,
    receiving_fumbles + rushing_fumbles + sack_fumbles as total_fumbles,
    receiving_fumbles_lost + rushing_fumbles_lost + sack_fumbles_lost as total_fumbles_lost,
    
    -- Data quality
    now() as dbt_loaded_at
    
from season_rankings
where player_id is not null