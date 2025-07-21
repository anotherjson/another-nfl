-- Test for data quality checks across staging models

-- Test: No duplicate game_ids in pbp data
select 'pbp_duplicate_games' as test_name, count(*) as failures
from (
    select game_id, play_id, count(*) as cnt
    from {{ ref('stg_pbp') }}
    group by game_id, play_id
    having count(*) > 1
)
union all

-- Test: All teams in weekly stats exist in team_desc
select 'weekly_invalid_teams' as test_name, count(*) as failures
from (
    select distinct w.team
    from {{ ref('stg_weekly') }} w
    left join {{ ref('stg_team_desc') }} t on w.team = t.team_id
    where t.team_id is null and w.team is not null
)
union all

-- Test: No negative fantasy points (excluding defensive players)
select 'negative_fantasy_points' as test_name, count(*) as failures  
from {{ ref('stg_weekly') }}
where fantasy_points_ppr < -5  -- Allow for some defensive fumbles/penalties
  and position_group != 'DEF'
union all

-- Test: Game dates are reasonable
select 'invalid_game_dates' as test_name, count(*) as failures
from {{ ref('stg_schedules') }}
where game_date < '1999-01-01' or game_date > current_date + interval '1 year'