{{ materialize_as_table_with_indexes('weekly') }}

-- Enhanced staging model for weekly player statistics
-- Optimized for Streamlit visualization and fantasy analysis

{{ create_staging_table_query(
  'weekly',
  "
    -- Player identifiers
    player_id,
    player_display_name as player_name,
    position,
    
    -- Team and season context
    recent_team as team,
    season,
    week,
    
    -- Game context
    opponent_team,
    
    -- Passing statistics
    coalesce(completions, 0) as completions,
    coalesce(attempts, 0) as attempts,
    coalesce(passing_yards, 0) as passing_yards,
    coalesce(passing_tds, 0) as passing_touchdowns,
    coalesce(interceptions, 0) as interceptions,
    
    -- Rushing statistics
    coalesce(carries, 0) as carries,
    coalesce(rushing_yards, 0) as rushing_yards,
    coalesce(rushing_tds, 0) as rushing_touchdowns,
    
    -- Receiving statistics
    coalesce(targets, 0) as targets,
    coalesce(receptions, 0) as receptions,
    coalesce(receiving_yards, 0) as receiving_yards,
    coalesce(receiving_tds, 0) as receiving_touchdowns,
    
    -- Fantasy relevant
    coalesce(fantasy_points, 0) as fantasy_points,
    coalesce(fantasy_points_ppr, 0) as fantasy_points_ppr,
    
    -- Additional metrics
    coalesce(target_share, 0) as target_share,
    coalesce(air_yards_share, 0) as air_yards_share,
    coalesce(wopr, 0) as wopr_score
  ",
  "
    -- Streamlit-friendly calculated fields
    case 
      when position in ('QB') then 'Quarterback'
      when position in ('RB', 'FB') then 'Running Back'
      when position in ('WR') then 'Wide Receiver'
      when position in ('TE') then 'Tight End'
      when position in ('K') then 'Kicker'
      when position in ('DEF') then 'Defense'
      else 'Other'
    end as position_group,
    
    -- Total touchdowns
    coalesce(passing_tds, 0) + coalesce(rushing_tds, 0) + coalesce(receiving_tds, 0) as total_touchdowns,
    
    -- Total yards
    coalesce(passing_yards, 0) + coalesce(rushing_yards, 0) + coalesce(receiving_yards, 0) as total_yards,
    
    -- Fantasy performance tier
    case 
      when fantasy_points_ppr >= 20 then 'Elite (20+)'
      when fantasy_points_ppr >= 15 then 'Great (15-19.9)'
      when fantasy_points_ppr >= 10 then 'Good (10-14.9)'
      when fantasy_points_ppr >= 5 then 'Okay (5-9.9)'
      when fantasy_points_ppr > 0 then 'Poor (0.1-4.9)'
      else 'Zero (0)'
    end as fantasy_performance_tier,
    
    -- Usage indicator
    case
      when position = 'QB' and attempts >= 30 then 'Heavy Usage'
      when position = 'QB' and attempts >= 20 then 'Moderate Usage'
      when position = 'QB' and attempts >= 10 then 'Light Usage'
      when position in ('RB', 'WR', 'TE') and targets + carries >= 15 then 'Heavy Usage'
      when position in ('RB', 'WR', 'TE') and targets + carries >= 8 then 'Moderate Usage'
      when position in ('RB', 'WR', 'TE') and targets + carries >= 3 then 'Light Usage'
      else 'Minimal Usage'
    end as usage_tier
  "
) }}