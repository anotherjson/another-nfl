# dbt Intermediate Models Documentation

Enhanced intermediate models for NFL analytics with EPA metrics, efficiency calculations, and advanced team/player insights.

## Models Overview

| Model | Description | Status | Tests |
|-------|-------------|--------|-------|
| `int_team_performance` | Team analytics with efficiency metrics | ✅ Production | 8/8 passing |
| `int_player_weekly_stats` | Player rankings and rolling analytics | ✅ Production | 9/9 passing |

**Total**: 2 models, 17/17 tests passing

## int_team_performance

Advanced team performance metrics aggregated by season and week.

**Materialization**: Table | **Tags**: `intermediate`, `team_performance`  
**Dependencies**: `stg_pbp`, `stg_schedules`, `stg_team_desc`

### Key Metrics
- **Down Conversion**: 3rd/4th down success rates with detailed breakdowns
- **EPA Analytics**: Expected Points Added per play (offense efficiency)
- **Play Distribution**: Pass/run rates and efficiency metrics
- **Win Tracking**: Dynamic win percentage based on season progress
- **Game Results**: Integration with schedule data for W/L/T records

### Essential Columns
| Column | Description |
|--------|-------------|
| `team_id`, `season`, `week` | Primary identifiers |
| `third_down_conversion_rate` | 3rd down success percentage |
| `avg_epa` | Average Expected Points Added per play |
| `win_percentage` | Season win percentage through current week |
| `pass_rate`, `run_rate` | Play-calling distribution percentages |
| `yards_per_play` | Average yards gained per offensive play |

### Usage Examples
```sql
-- Team efficiency leaders
SELECT team_id, team_name, avg_epa, third_down_conversion_rate
FROM {{ ref('int_team_performance') }}
WHERE season = 2023 AND week = 10
ORDER BY avg_epa DESC
LIMIT 10;

-- Season progression analysis
SELECT team_id, week, win_percentage, avg_epa
FROM {{ ref('int_team_performance') }}
WHERE season = 2023 AND team_id = 'KC'
ORDER BY week;
```

---

## int_player_weekly_stats

Comprehensive player analytics with position rankings, rolling averages, and EPA efficiency metrics.

**Materialization**: Table | **Tags**: `intermediate`, `player_stats`  
**Dependencies**: `stg_weekly`, `stg_team_desc`

### Key Metrics
- **Position Rankings**: Weekly and season-to-date rankings by position group
- **Rolling Analytics**: 4-week fantasy point averages for trend analysis
- **EPA Efficiency**: Expected Points Added per opportunity (target/carry/attempt)
- **Air Yards Analytics**: Target depth, YAC, and conversion efficiency
- **Comprehensive Stats**: All receiving, rushing, passing, and special teams metrics

### Essential Columns
| Column | Description |
|--------|-------------|
| `player_id`, `season`, `week` | Primary identifiers |
| `position`, `position_group` | Player position information |
| `fantasy_points_ppr_4wk_avg` | 4-week rolling PPR average |
| `receiving_epa_per_target` | EPA efficiency on receiving targets |
| `position_rank_week` | Weekly ranking within position group |
| `air_yards_per_target` | Average target depth |
| `catch_rate` | Receptions per target percentage |

### Advanced EPA Metrics
```sql
-- EPA efficiency leaders by position
SELECT 
    position,
    player_name,
    receiving_epa_per_target,
    rushing_epa_per_carry,
    targets,
    carries
FROM {{ ref('int_player_weekly_stats') }}
WHERE season = 2023 AND week >= 8
  AND (targets >= 5 OR carries >= 10)
ORDER BY position, receiving_epa_per_target DESC;
```

### Rolling Trends Analysis
```sql
-- Trending fantasy performers
SELECT 
    player_name,
    position,
    fantasy_points_ppr,
    fantasy_points_ppr_4wk_avg,
    (fantasy_points_ppr - fantasy_points_ppr_4wk_avg) as weekly_variance
FROM {{ ref('int_player_weekly_stats') }}
WHERE season = 2023 AND week = 10
  AND fantasy_points_ppr_4wk_avg > 10
ORDER BY weekly_variance DESC
LIMIT 15;
```

### Position Rankings
```sql
-- Top weekly performers by position
SELECT 
    position_group,
    player_name,
    fantasy_points_ppr,
    position_rank_week
FROM {{ ref('int_player_weekly_stats') }}
WHERE season = 2023 AND week = 10
  AND position_rank_week <= 5
ORDER BY position_group, position_rank_week;
```

## Data Quality & Testing

### Test Coverage (17/17 passing)
**int_team_performance**:
- ✅ Key identifier constraints (team_id, season, week)
- ✅ Rate validations (0-1 range for percentages)
- ✅ Play count validation (0-150 range)
- ✅ Foreign key relationships

**int_player_weekly_stats**:
- ✅ Player identifier constraints (player_id, season, week)
- ✅ Fantasy points range validation
- ✅ Rate percentage validations (0-1)
- ✅ Team relationship validation

### Performance Optimization
- **Table materialization** for fast query performance
- **Indexed on** player_id/team_id + season + week
- **Partitioning recommended** by season for large datasets
- **Incremental loading** supported for production workflows

## Integration Patterns

### CLI Model Operations
```bash
# Query models directly
uv run python -m src.cli models query int_team_performance --limit 10
uv run python -m src.cli models query int_player_weekly_stats --limit 20

# Time travel queries
uv run python -m src.cli models query int_team_performance --as-of-date 2024-12-01
```

### Custom Analytics
```sql
-- Cross-model team analysis
WITH team_offense AS (
  SELECT team_id, season, AVG(avg_epa) as offense_epa
  FROM {{ ref('int_team_performance') }}
  WHERE season = 2023
  GROUP BY team_id, season
),
team_players AS (
  SELECT team, season, COUNT(DISTINCT player_id) as active_players
  FROM {{ ref('int_player_weekly_stats') }}
  WHERE season = 2023 AND fantasy_points_ppr > 0
  GROUP BY team, season
)
SELECT 
  t.team_id,
  t.offense_epa,
  p.active_players
FROM team_offense t
JOIN team_players p ON t.team_id = p.team
ORDER BY t.offense_epa DESC;
```

## Best Practices

### Query Guidelines
1. **Always filter by season** for optimal performance
2. **Use position_group for broader analysis**, position for specific roles
3. **Leverage rolling averages** for trend identification
4. **Focus on EPA metrics** for advanced analytics
5. **Use rankings** for relative performance assessment

### Common Anti-Patterns
- ❌ Querying without season filter (slow performance)
- ❌ Comparing raw fantasy points across different seasons
- ❌ Ignoring EPA metrics in efficiency analysis
- ❌ Using only single-week data for trend analysis

## Future Enhancements

### Planned Improvements
- **Game-level analysis model** with betting integration
- **Advanced metrics integration** (Next Gen Stats, PFF grades)
- **Machine learning features** for predictive modeling
- **Real-time updates** for live game integration

### Model Extensions
- **Seasonal aggregations** for year-end analysis
- **Playoff-specific models** for post-season analytics
- **Injury impact analysis** with health data integration
- **Weather adjustments** for outdoor game conditions

---

*Last Updated: July 23, 2025*  
*Models Status: Production Ready ✅*  
*Test Success Rate: 17/17 (100%) ✅*