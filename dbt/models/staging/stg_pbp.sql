{{ materialize_as_table_with_indexes('pbp') }}

-- Enhanced staging model for play-by-play data
-- Optimized for Streamlit visualization and analytics

{{ create_staging_table_query(
  'pbp',
  "
    -- Game identifiers
    game_id,
    play_id,
    
    -- Game context
    season,
    week,
    home_team,
    away_team,
    
    -- Play details
    down,
    ydstogo as yards_to_go,
    yardline_100 as yards_from_endzone,
    yards_gained,
    play_type,
    
    -- Advanced metrics (for analytics)
    coalesce(epa, 0) as expected_points_added,
    coalesce(wp, 0.5) as win_probability,
    coalesce(wpa, 0) as win_probability_added,
    
    -- Player information
    passer_player_name as passer,
    rusher_player_name as rusher, 
    receiver_player_name as receiver,
    
    -- Scoring plays
    touchdown,
    field_goal_result,
    safety,
    
    -- Time information
    quarter_seconds_remaining,
    half_seconds_remaining,
    game_seconds_remaining,
    
    -- Situation
    score_differential,
    posteam as possession_team,
    defteam as defense_team,
    
    -- Play results
    complete_pass,
    interception,
    fumble_lost,
    penalty,
    
    -- Fantasy relevant
    passing_yards,
    rushing_yards,
    receiving_yards,
    target,
    reception
  ",
  "
    -- Streamlit-friendly calculated fields
    case 
      when touchdown = 1 then 'Touchdown'
      when field_goal_result = 'made' then 'Field Goal'
      when interception = 1 then 'Interception'
      when fumble_lost = 1 then 'Fumble'
      when penalty = 1 then 'Penalty'
      else 'Standard Play'
    end as play_outcome_category,
    
    case 
      when down is null then 'Special Teams'
      when down = 1 then '1st Down'
      when down = 2 then '2nd Down' 
      when down = 3 then '3rd Down'
      when down = 4 then '4th Down'
      else 'Unknown'
    end as down_display,
    
    case
      when yards_gained >= 20 then 'Big Gain (20+)'
      when yards_gained >= 10 then 'Good Gain (10-19)'
      when yards_gained >= 1 then 'Short Gain (1-9)'
      when yards_gained = 0 then 'No Gain'
      else 'Loss'
    end as yardage_category
  "
) }}