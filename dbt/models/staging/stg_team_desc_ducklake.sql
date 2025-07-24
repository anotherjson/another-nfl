{{ config(
    materialized='view',
    tags=['staging', 'ducklake']
) }}

-- Staging model for NFL team descriptions using DuckLake-managed Parquet files
WITH team_descriptions AS (
    -- Read directly from DuckLake-managed Parquet file
    -- Future enhancement: Use DuckLake time travel via UDF
    SELECT * FROM read_parquet('../data/team_desc/etl_date=2025-07-21/data.parquet')
),

final AS (
    SELECT
        team_abbr as team_id,
        team_name,
        team_nick,
        team_id as nfl_team_id,
        team_conf as team_conference,
        team_division,
        team_league_logo,
        team_wordmark,
        team_logo_espn,
        team_logo_wikipedia,
        team_conference_logo,
        team_logo_squared,
        
        -- Add DuckLake metadata
        now() as dbt_loaded_at,
        'ducklake' as data_source,
        1 as data_version
        
    FROM team_descriptions
)

SELECT * FROM final