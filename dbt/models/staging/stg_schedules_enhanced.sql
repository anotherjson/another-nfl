{{ materialize_as_table_with_indexes('schedules') }}

-- Enhanced staging model for NFL schedules
-- Optimized for Streamlit visualization and game analysis

{{ create_staging_table_query(
  'schedules',
  "
    -- Game identifiers
    game_id,
    
    -- Season and timing
    season,
    week,
    gameday as game_date,
    gametime as game_time,
    
    -- Teams
    home_team,
    away_team,
    
    -- Scores
    home_score,
    away_score,
    
    -- Game information
    game_type,
    roof as stadium_type,
    surface as field_surface,
    temp as temperature,
    wind,
    
    -- Location
    stadium as stadium_name,
    
    -- Betting information
    spread_line,
    total_line,
    
    -- Additional context
    neutral_site,
    playoff,
    div_game as divisional_game,
    
    -- Weather conditions
    weather,
    humidity,
    wind_speed
  ",
  "
    -- Streamlit-friendly calculated fields
    case 
      when home_score > away_score then home_team
      when away_score > home_score then away_team
      else null
    end as winning_team,
    
    case 
      when home_score > away_score then away_team
      when away_score > home_score then home_team  
      else null
    end as losing_team,
    
    abs(home_score - away_score) as score_differential,
    home_score + away_score as total_points,
    
    case
      when abs(home_score - away_score) <= 3 then 'Close Game (0-3 pts)'
      when abs(home_score - away_score) <= 7 then 'Competitive (4-7 pts)'
      when abs(home_score - away_score) <= 14 then 'Moderate (8-14 pts)'
      when abs(home_score - away_score) <= 21 then 'Decisive (15-21 pts)'
      else 'Blowout (22+ pts)'
    end as game_competitiveness,
    
    case
      when game_type = 'REG' then 'Regular Season'
      when game_type = 'WC' then 'Wild Card'
      when game_type = 'DIV' then 'Divisional Round'
      when game_type = 'CON' then 'Conference Championship'
      when game_type = 'SB' then 'Super Bowl'
      else game_type
    end as game_type_display,
    
    case
      when week <= 4 then 'Early Season (Weeks 1-4)'
      when week <= 8 then 'Mid Season (Weeks 5-8)'
      when week <= 12 then 'Late Season (Weeks 9-12)'
      when week <= 17 then 'Season Finale (Weeks 13-17)'
      else 'Playoffs'
    end as season_phase,
    
    -- Stadium information for Streamlit
    case
      when roof in ('dome', 'closed') then 'Indoor'
      when roof = 'retractable' then 'Retractable Roof'
      when roof in ('outdoors', 'open') then 'Outdoor'
      else 'Unknown'
    end as stadium_type_display,
    
    case
      when surface in ('grass', 'natural_grass') then 'Natural Grass'
      when surface in ('artificial', 'fieldturf', 'astroturf') then 'Artificial Turf'
      else coalesce(surface, 'Unknown')
    end as field_surface_display,
    
    -- Weather categories for visualization
    case
      when temperature >= 80 then 'Hot (80°F+)'
      when temperature >= 60 then 'Warm (60-79°F)'
      when temperature >= 40 then 'Cool (40-59°F)'
      when temperature >= 20 then 'Cold (20-39°F)'
      when temperature < 20 then 'Freezing (<20°F)'
      else 'Indoor/Unknown'
    end as temperature_category,
    
    -- Betting analysis
    case
      when spread_line > 0 then away_team || ' favored by ' || abs(spread_line)
      when spread_line < 0 then home_team || ' favored by ' || abs(spread_line)
      else 'Pick\'em'
    end as betting_favorite
  "
) }}