/*
Sample SQL query for dbt model analysis
Usage: uv run python -m src.cli models sql --file queries/team_analysis.sql
*/

-- NFL Team Conference and Division Analysis
WITH team_stats AS (
    SELECT 
        team_conference,
        team_division,
        team_name,
        team_abbr as team_id,
        team_nick
    FROM read_parquet('data/team_desc/etl_date=*/data.parquet')
    WHERE team_conference IS NOT NULL
),

conference_summary AS (
    SELECT 
        team_conference,
        COUNT(*) as total_teams,
        STRING_AGG(team_division, ', ') as divisions
    FROM (
        SELECT DISTINCT team_conference, team_division
        FROM team_stats
        ORDER BY team_conference, team_division
    ) div_summary
    GROUP BY team_conference
),

division_summary AS (
    SELECT 
        team_division,
        team_conference,
        COUNT(*) as teams_in_division,
        STRING_AGG(team_name, ', ' ORDER BY team_name) as team_names
    FROM team_stats
    GROUP BY team_division, team_conference
    ORDER BY team_conference, team_division
)

-- Main Results: Conference and Division Breakdown
SELECT 
    '=== NFL CONFERENCE SUMMARY ===' as section,
    '' as team_conference,
    '' as details,
    '' as count
    
UNION ALL

SELECT 
    '' as section,
    team_conference,
    CAST(total_teams as VARCHAR) || ' teams' as details,
    CAST(total_teams as VARCHAR) as count
FROM conference_summary
ORDER BY team_conference

UNION ALL

SELECT 
    '=== DIVISION BREAKDOWN ===' as section,
    '' as team_conference, 
    '' as details,
    '' as count

UNION ALL

SELECT
    '' as section,
    div.team_conference || ' - ' || div.team_division as team_conference,
    CAST(div.teams_in_division as VARCHAR) || ' teams: ' || 
    CASE 
        WHEN LENGTH(div.team_names) > 50 
        THEN SUBSTRING(div.team_names, 1, 47) || '...'
        ELSE div.team_names 
    END as details,
    CAST(div.teams_in_division as VARCHAR) as count
FROM division_summary div
ORDER BY 2;