# dbt Intermediate Models Documentation

This document provides comprehensive documentation for the enhanced intermediate models in the NFL Analytics dbt project.

## Overview

The intermediate models serve as the analytical layer between raw staging data and final marts, providing enriched NFL analytics with advanced metrics, efficiency calculations, and position-specific insights. These models leverage the enhanced staging layer to deliver production-ready analytics capabilities.

## Enhanced Models

### 1. `int_team_performance`
**Description:** Enhanced team performance metrics aggregated by season and week with advanced analytics

**Materialized as:** Table  
**Tags:** `intermediate`, `team_performance`  
**Dependencies:** `stg_pbp`, `stg_schedules`, `stg_team_desc`

#### Key Features
- **Advanced Down Situation Analytics**: Third and fourth down conversion rates with detailed breakdowns
- **Enhanced Scoring Metrics**: Touchdowns, field goal attempts, safeties with EPA/WPA analytics
- **Dynamic Win Percentage**: Season-progress-based win percentage calculations
- **Play-Calling Analysis**: Detailed pass/run distribution and efficiency metrics
- **Team Records Integration**: Wins, losses, ties with schedule data integration

#### Key Columns
| Column | Description | Type |
|--------|-------------|------|
| `team_id` | Team abbreviation (FK to stg_team_desc) | STRING |
| `season` | NFL season year | INTEGER |
| `week` | Week number within season | INTEGER |
| `total_plays` | Total offensive plays | INTEGER |
| `pass_rate` | Percentage of plays that are passes | DECIMAL(3) |
| `run_rate` | Percentage of plays that are runs | DECIMAL(3) |
| `third_down_conversion_rate` | Third down success percentage | DECIMAL(3) |
| `fourth_down_conversion_rate` | Fourth down success percentage | DECIMAL(3) |
| `yards_per_play` | Average yards gained per play | DECIMAL(2) |
| `avg_epa` | Average Expected Points Added per play | DECIMAL(4) |
| `total_epa` | Total Expected Points Added | DECIMAL(4) |
| `win_percentage` | Win percentage through current week | DECIMAL(3) |

#### Sample Query
```sql
SELECT 
    team_id,
    team_name,
    season,
    week,
    total_plays,
    pass_rate,
    run_rate,
    third_down_conversion_rate,
    avg_epa,
    win_percentage
FROM {{ ref('int_team_performance') }}
WHERE season = 2023 
  AND week <= 10
ORDER BY avg_epa DESC;
```

---

### 2. `int_player_weekly_stats`
**Description:** Enhanced weekly player statistics with advanced position-specific metrics and EPA analytics

**Materialized as:** Table  
**Tags:** `intermediate`, `player_stats`  
**Dependencies:** `stg_weekly`, `stg_team_desc`

#### Key Features
- **Comprehensive Statistics**: All receiving, rushing, passing, and special teams metrics
- **Advanced Efficiency Metrics**: EPA per opportunity, air yards analytics, YAC metrics
- **Rolling Analytics**: 4-week rolling average fantasy points for trend analysis
- **Position Rankings**: Weekly and season-to-date rankings by position and position group
- **Opponent Tracking**: Matchup analysis with opponent team data
- **Fumble Tracking**: Complete fumble and turnover analytics

#### Key Columns
| Column | Description | Type |
|--------|-------------|------|
| `player_id` | Unique player identifier | STRING |
| `player_name` | Player display name | STRING |
| `position` | Specific position (QB, RB, WR, etc.) | STRING |
| `position_group` | Position group (QB, RB, WR, TE, K) | STRING |
| `team` | Player's team (FK to stg_team_desc) | STRING |
| `opponent_team` | Opponent team for the week | STRING |
| `season` | NFL season year | INTEGER |
| `week` | Week number within season | INTEGER |
| `fantasy_points_ppr` | PPR fantasy points for the week | DECIMAL |
| `fantasy_points_ppr_4wk_avg` | 4-week rolling average PPR points | DECIMAL |
| `receiving_epa` | Expected Points Added on receiving plays | DECIMAL |
| `rushing_epa` | Expected Points Added on rushing plays | DECIMAL |
| `passing_epa` | Expected Points Added on passing plays | DECIMAL |
| `receiving_epa_per_target` | EPA per receiving target | DECIMAL(3) |
| `rushing_epa_per_carry` | EPA per rushing attempt | DECIMAL(3) |
| `passing_epa_per_attempt` | EPA per passing attempt | DECIMAL(3) |
| `air_yards_per_target` | Air yards per receiving target | DECIMAL(2) |
| `yac_per_reception` | Yards after catch per reception | DECIMAL(2) |
| `receiving_air_yards_share` | Percentage of air yards converted to receiving yards | DECIMAL(3) |
| `position_rank_week` | Weekly position group ranking | INTEGER |
| `specific_position_rank_week` | Weekly specific position ranking | INTEGER |
| `position_rank_std` | Season-to-date position ranking | INTEGER |

#### Advanced Metrics Explained

**EPA Metrics:**
- `receiving_epa_per_target`: Measures efficiency of targets (higher = better)
- `rushing_epa_per_carry`: Measures rushing efficiency (higher = better)  
- `passing_epa_per_attempt`: Measures passing efficiency (higher = better)

**Air Yards Analytics:**
- `air_yards_per_target`: Average air yards on targeted passes
- `receiving_air_yards_share`: How well player converts air yards to actual yards
- `yac_per_reception`: Average yards gained after catching the ball

**Rolling Metrics:**
- `fantasy_points_ppr_4wk_avg`: Smoothed fantasy performance for trend analysis

#### Sample Queries

**Top Weekly Performers by Position:**
```sql
SELECT 
    player_name,
    position,
    team,
    fantasy_points_ppr,
    position_rank_week,
    receiving_epa_per_target,
    rushing_epa_per_carry
FROM {{ ref('int_player_weekly_stats') }}
WHERE season = 2023 
  AND week = 10
  AND position_rank_week <= 10
ORDER BY position_group, position_rank_week;
```

**Trending Players (4-week average):**
```sql
SELECT 
    player_name,
    position,
    team,
    fantasy_points_ppr,
    fantasy_points_ppr_4wk_avg,
    (fantasy_points_ppr - fantasy_points_ppr_4wk_avg) as weekly_variance
FROM {{ ref('int_player_weekly_stats') }}
WHERE season = 2023 
  AND week >= 4
  AND fantasy_points_ppr_4wk_avg > 10
ORDER BY weekly_variance DESC
LIMIT 20;
```

**Efficiency Leaders:**
```sql
SELECT 
    player_name,
    position,
    team,
    targets,
    receiving_epa_per_target,
    air_yards_per_target,
    yac_per_reception,
    receiving_air_yards_share
FROM {{ ref('int_player_weekly_stats') }}
WHERE season = 2023 
  AND targets >= 5
  AND receiving_epa_per_target IS NOT NULL
ORDER BY receiving_epa_per_target DESC
LIMIT 25;
```

## Data Quality & Testing

### Test Coverage
Both intermediate models include comprehensive data quality tests:

**int_team_performance:**
- ✅ Not null constraints on key identifiers
- ✅ Range validation for rates and percentages (0-1)
- ✅ Range validation for total plays (0-150)
- ✅ Foreign key relationships to team reference data

**int_player_weekly_stats:**
- ✅ Not null constraints on player and game identifiers  
- ✅ Range validation for fantasy points and averages
- ✅ Range validation for rates and percentages (0-1)
- ✅ Foreign key relationships to team reference data

### Performance Considerations
- **Materialized as Tables**: Both models are materialized as tables for optimal query performance
- **Indexing**: Primary keys on player_id/team_id + season + week for efficient filtering
- **Partitioning**: Consider partitioning by season for large datasets
- **Incremental Loads**: Models support incremental loading strategies

## Usage Guidelines

### Best Practices
1. **Filtering**: Always filter by season and week for optimal performance
2. **Position Analysis**: Use `position_group` for broader analysis, `position` for specific roles
3. **Trend Analysis**: Leverage 4-week rolling averages for smoother trend identification
4. **Efficiency Metrics**: Focus on EPA metrics for advanced analytics
5. **Rankings**: Use position rankings for relative performance assessment

### Common Patterns
```sql
-- Team performance over time
SELECT team_id, week, avg_epa, win_percentage
FROM {{ ref('int_team_performance') }}
WHERE season = 2023
ORDER BY team_id, week;

-- Player breakout identification  
SELECT player_name, position, fantasy_points_ppr_4wk_avg
FROM {{ ref('int_player_weekly_stats') }}
WHERE season = 2023 
  AND week >= 8
  AND fantasy_points_ppr_4wk_avg > (
    SELECT AVG(fantasy_points_ppr_4wk_avg) 
    FROM {{ ref('int_player_weekly_stats') }}
    WHERE position_group = 'WR' AND season = 2023
  );
```

## Integration with Marts Layer

These intermediate models serve as the foundation for downstream mart models:
- **Team Performance**: Feeds into season-long team analytics and rankings
- **Player Statistics**: Enables position-specific marts and fantasy analytics
- **Cross-Model Joins**: Support comprehensive game and matchup analysis

## Future Enhancements

Planned improvements include:
- **Game-level Analysis**: Re-implementation of game analysis model with betting data
- **Advanced Metrics**: Integration of Next Gen Stats and tracking data
- **Machine Learning Features**: Feature engineering for predictive models
- **Real-time Updates**: Support for live game data integration

---

*Last Updated: July 23, 2025*  
*dbt Version: 1.10.4*  
*Models Status: Production Ready ✅*