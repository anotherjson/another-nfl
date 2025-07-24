{{ materialize_as_table_with_indexes('team_desc') }}

-- Enhanced staging model for team descriptions
-- Optimized for Streamlit visualization and reference lookups

{{ create_staging_table_query(
  'team_desc',
  "
    -- Team identifiers
    team_abbr,
    team_name,
    team_id,
    
    -- League structure
    team_conf as team_conference,
    team_division,
    
    -- Visual branding
    team_color as primary_color,
    team_color2 as secondary_color,
    team_color3 as tertiary_color,
    team_color4 as quaternary_color,
    
    -- Logo information
    team_logo_espn,
    team_logo_wikipedia,
    team_wordmark,
    
    -- Team nickname (since team_city not available)
    team_nick
  ",
  "
    -- Streamlit-friendly calculated fields
    team_name as full_team_name,
    
    case 
      when team_conf = 'AFC' then 'American Football Conference'
      when team_conf = 'NFC' then 'National Football Conference'
      else team_conf
    end as conference_full_name,
    
    case
      when team_division = 'AFC East' then 'AFC East'
      when team_division = 'AFC North' then 'AFC North' 
      when team_division = 'AFC South' then 'AFC South'
      when team_division = 'AFC West' then 'AFC West'
      when team_division = 'NFC East' then 'NFC East'
      when team_division = 'NFC North' then 'NFC North'
      when team_division = 'NFC South' then 'NFC South'
      when team_division = 'NFC West' then 'NFC West'
      else team_division
    end as division_display,
    
    -- For Streamlit color mapping
    coalesce(team_color, '#808080') as chart_color_primary,
    coalesce(team_color2, team_color, '#404040') as chart_color_secondary,
    
    -- Logo URL (prefer ESPN, fallback to Wikipedia)
    coalesce(team_logo_espn, team_logo_wikipedia) as display_logo,
    
    -- Conference/Division sorting
    case team_conf
      when 'AFC' then 1
      when 'NFC' then 2
      else 3
    end as conference_sort_order,
    
    case team_division
      when 'AFC East' then 1
      when 'AFC North' then 2
      when 'AFC South' then 3
      when 'AFC West' then 4
      when 'NFC East' then 5
      when 'NFC North' then 6
      when 'NFC South' then 7
      when 'NFC West' then 8
      else 9
    end as division_sort_order
  "
) }}

-- Add row number for deduplication (in case of multiple ETL dates)
-- Keep only the most recent version
qualify row_number() over (partition by team_abbr order by dbt_loaded_at desc) = 1