{{ config(
    materialized='table',
    description='Team performance metrics aggregated by season and week'
) }}

with pbp_team_stats as (
    select
        season,
        week,
        possession_team as team_id,
        count(*) as total_plays,
        sum(case when play_type = 'pass' then 1 else 0 end) as pass_plays,
        sum(case when play_type = 'run' then 1 else 0 end) as run_plays,
        sum(yards_gained) as total_yards,
        sum(case when play_type = 'pass' then yards_gained else 0 end) as passing_yards,
        sum(case when play_type = 'run' then yards_gained else 0 end) as rushing_yards,
        sum(case when touchdown = 1 then 1 else 0 end) as touchdowns,
        avg(epa) as avg_epa,
        sum(epa) as total_epa,
        count(distinct game_id) as games_played
    from {{ ref('stg_pbp') }}
    where possession_team is not null
    group by season, week, possession_team
),

schedule_results as (
    select
        season,
        week,
        home_team,
        away_team,
        home_score,
        away_score,
        case 
            when home_score > away_score then home_team
            when away_score > home_score then away_team
            else null
        end as winning_team,
        case
            when home_score = away_score then 1
            else 0
        end as is_tie
    from {{ ref('stg_schedules') }}
    where home_score is not null and away_score is not null
),

team_records as (
    select 
        season,
        week,
        team_id,
        sum(case when team_id = winning_team then 1 else 0 end) as wins,
        sum(case when (team_id = home_team or team_id = away_team) and winning_team is not null and team_id != winning_team then 1 else 0 end) as losses,
        sum(case when (team_id = home_team or team_id = away_team) and is_tie = 1 then 1 else 0 end) as ties
    from (
        select season, week, home_team as team_id, winning_team, is_tie, home_team, away_team from schedule_results
        union all
        select season, week, away_team as team_id, winning_team, is_tie, home_team, away_team from schedule_results
    )
    group by season, week, team_id
)

select
    p.season,
    p.week,
    p.team_id,
    t.team_name,
    t.conference,
    t.division,
    
    -- Play calling metrics
    p.total_plays,
    p.pass_plays,
    p.run_plays,
    round(p.pass_plays::float / nullif(p.total_plays, 0), 3) as pass_rate,
    round(p.run_plays::float / nullif(p.total_plays, 0), 3) as run_rate,
    
    -- Yardage metrics
    p.total_yards,
    p.passing_yards,
    p.rushing_yards,
    round(p.total_yards::float / nullif(p.total_plays, 0), 2) as yards_per_play,
    round(p.passing_yards::float / nullif(p.pass_plays, 0), 2) as yards_per_pass,
    round(p.rushing_yards::float / nullif(p.run_plays, 0), 2) as yards_per_rush,
    
    -- Scoring and efficiency
    p.touchdowns,
    round(p.avg_epa, 4) as avg_epa,
    p.total_epa,
    
    -- Record
    coalesce(r.wins, 0) as wins,
    coalesce(r.losses, 0) as losses, 
    coalesce(r.ties, 0) as ties,
    
    -- Data quality
    p.games_played,
    current_timestamp() as dbt_loaded_at
    
from pbp_team_stats p
left join {{ ref('stg_team_desc') }} t on p.team_id = t.team_id
left join team_records r on p.season = r.season and p.week = r.week and p.team_id = r.team_id
where p.team_id is not null