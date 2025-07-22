# Fantasy Football Analytics

Comprehensive fantasy football analysis with player rankings, projections, and matchup insights.

---

## Fantasy Leaders by Position

```sql fantasy_leaders_by_position
SELECT 
    position,
    player_name,
    team,
    ROUND(SUM(fantasy_points_ppr), 1) as total_fantasy_points,
    ROUND(AVG(fantasy_points_ppr), 1) as avg_fantasy_points,
    COUNT(*) as games_played,
    ROW_NUMBER() OVER (PARTITION BY position ORDER BY SUM(fantasy_points_ppr) DESC) as position_rank
FROM int_player_weekly_stats
WHERE season = 2023 AND fantasy_points_ppr > 0
GROUP BY position, player_name, team
HAVING COUNT(*) >= 8
ORDER BY position, total_fantasy_points DESC
```

<DataTable 
    data={fantasy_leaders_by_position}
    search=true
    groupBy="position"
/>

---

## Weekly Fantasy Trends

```sql weekly_fantasy_trends
SELECT 
    week,
    ROUND(AVG(fantasy_points_ppr), 2) as avg_fantasy_points,
    ROUND(AVG(CASE WHEN position = 'QB' THEN fantasy_points_ppr END), 2) as avg_qb_points,
    ROUND(AVG(CASE WHEN position = 'RB' THEN fantasy_points_ppr END), 2) as avg_rb_points,
    ROUND(AVG(CASE WHEN position = 'WR' THEN fantasy_points_ppr END), 2) as avg_wr_points,
    ROUND(AVG(CASE WHEN position = 'TE' THEN fantasy_points_ppr END), 2) as avg_te_points
FROM int_player_weekly_stats
WHERE season = 2023 AND fantasy_points_ppr > 0
GROUP BY week
ORDER BY week
```

<LineChart 
    data={weekly_fantasy_trends}
    x="week"
    y={["avg_qb_points", "avg_rb_points", "avg_wr_points", "avg_te_points"]}
    title="Average Fantasy Points by Position and Week"
    yAxisTitle="Average Fantasy Points"
/>

---

## Consistency vs Ceiling Analysis

```sql player_consistency
SELECT 
    player_name,
    position,
    team,
    ROUND(AVG(fantasy_points_ppr), 2) as avg_points,
    ROUND(STDDEV(fantasy_points_ppr), 2) as point_stddev,
    ROUND(MAX(fantasy_points_ppr), 1) as best_game,
    ROUND(MIN(fantasy_points_ppr), 1) as worst_game,
    COUNT(*) as games_played,
    ROUND(AVG(fantasy_points_ppr) / NULLIF(STDDEV(fantasy_points_ppr), 0), 2) as consistency_score
FROM int_player_weekly_stats
WHERE season = 2023 AND fantasy_points_ppr > 0
GROUP BY player_name, position, team
HAVING COUNT(*) >= 10 AND STDDEV(fantasy_points_ppr) > 0
ORDER BY consistency_score DESC
LIMIT 50
```

<ScatterChart 
    data={player_consistency}
    x="consistency_score"
    y="best_game"
    series="position"
    size="avg_points"
    title="Player Consistency vs Ceiling (Bubble Size = Average Points)"
    xAxisTitle="Consistency Score (Higher = More Consistent)"
    yAxisTitle="Best Single Game"
/>

---

## Target Share Analysis

```sql target_analysis
SELECT 
    player_name,
    position,
    team,
    SUM(targets) as total_targets,
    SUM(receptions) as total_receptions,
    ROUND(SUM(receptions)::FLOAT / NULLIF(SUM(targets), 0) * 100, 1) as catch_rate,
    ROUND(SUM(receiving_yards)::FLOAT / NULLIF(SUM(receptions), 0), 1) as yards_per_reception,
    SUM(receiving_touchdowns) as receiving_tds,
    ROUND(SUM(fantasy_points_ppr), 1) as total_fantasy_points
FROM int_player_weekly_stats
WHERE season = 2023 AND position IN ('WR', 'TE', 'RB') AND targets > 0
GROUP BY player_name, position, team
HAVING SUM(targets) >= 40
ORDER BY total_targets DESC
LIMIT 30
```

<DataTable 
    data={target_analysis}
    search=true
/>

---

## Position Depth Analysis

<div class="grid grid-cols-1 md:grid-cols-2 gap-8">

### Wide Receiver Depth
```sql wr_depth
SELECT 
    ROW_NUMBER() OVER (ORDER BY SUM(fantasy_points_ppr) DESC) as rank,
    player_name,
    team,
    ROUND(SUM(fantasy_points_ppr), 1) as total_points,
    ROUND(AVG(fantasy_points_ppr), 1) as avg_points
FROM int_player_weekly_stats
WHERE season = 2023 AND position = 'WR' AND fantasy_points_ppr > 0
GROUP BY player_name, team
HAVING COUNT(*) >= 8
ORDER BY total_points DESC
LIMIT 24
```

<BarChart 
    data={wr_depth}
    x="rank"
    y="total_points"
    title="Top 24 WR Fantasy Points"
    yAxisTitle="Total Fantasy Points"
/>

### Running Back Depth
```sql rb_depth
SELECT 
    ROW_NUMBER() OVER (ORDER BY SUM(fantasy_points_ppr) DESC) as rank,
    player_name,
    team,
    ROUND(SUM(fantasy_points_ppr), 1) as total_points,
    ROUND(AVG(fantasy_points_ppr), 1) as avg_points
FROM int_player_weekly_stats
WHERE season = 2023 AND position = 'RB' AND fantasy_points_ppr > 0
GROUP BY player_name, team
HAVING COUNT(*) >= 8
ORDER BY total_points DESC
LIMIT 24
```

<BarChart 
    data={rb_depth}
    x="rank"
    y="total_points"
    title="Top 24 RB Fantasy Points"
    yAxisTitle="Total Fantasy Points"
/>

</div>

---

## Rookie Performance

```sql rookie_performance
SELECT 
    player_name,
    position,
    team,
    ROUND(SUM(fantasy_points_ppr), 1) as total_points,
    ROUND(AVG(fantasy_points_ppr), 1) as avg_points,
    COUNT(*) as games_played,
    ROUND(SUM(fantasy_points_ppr) / COUNT(*), 1) as points_per_game
FROM int_player_weekly_stats p
WHERE season = 2023 AND fantasy_points_ppr > 0
    AND EXISTS (
        SELECT 1 FROM stg_pbp_ducklake pbp 
        WHERE pbp.season = 2023 
        AND (pbp.passer_player_name = p.player_name 
             OR pbp.rusher_player_name = p.player_name 
             OR pbp.receiver_player_name = p.player_name)
        LIMIT 1
    )
GROUP BY player_name, position, team
HAVING COUNT(*) >= 4
ORDER BY total_points DESC
LIMIT 15
```

<DataTable 
    data={rookie_performance}
    title="Top Rookie Fantasy Performers"
/>

---

## Team Fantasy Production

```sql team_fantasy
SELECT 
    team,
    ROUND(SUM(CASE WHEN position = 'QB' THEN fantasy_points_ppr END), 1) as qb_points,
    ROUND(SUM(CASE WHEN position = 'RB' THEN fantasy_points_ppr END), 1) as rb_points,
    ROUND(SUM(CASE WHEN position = 'WR' THEN fantasy_points_ppr END), 1) as wr_points,
    ROUND(SUM(CASE WHEN position = 'TE' THEN fantasy_points_ppr END), 1) as te_points,
    ROUND(SUM(fantasy_points_ppr), 1) as total_points
FROM int_player_weekly_stats
WHERE season = 2023 AND fantasy_points_ppr > 0
GROUP BY team
ORDER BY total_points DESC
```

<BarChart 
    data={team_fantasy}
    x="team"
    y={["qb_points", "rb_points", "wr_points", "te_points"]}
    type="stacked"
    title="Team Fantasy Production by Position"
    yAxisTitle="Total Fantasy Points"
/>

---

## Playoff Fantasy Outlook

```sql fantasy_playoffs
WITH recent_performance AS (
    SELECT 
        player_name,
        position,
        team,
        ROUND(AVG(CASE WHEN week >= 14 THEN fantasy_points_ppr END), 1) as playoff_weeks_avg,
        ROUND(AVG(fantasy_points_ppr), 1) as season_avg,
        COUNT(CASE WHEN week >= 14 THEN 1 END) as playoff_weeks_played,
        COUNT(*) as total_games
    FROM int_player_weekly_stats
    WHERE season = 2023 AND fantasy_points_ppr > 0
    GROUP BY player_name, position, team
    HAVING COUNT(CASE WHEN week >= 14 THEN 1 END) >= 3
        AND COUNT(*) >= 10
)
SELECT 
    player_name,
    position,
    team,
    season_avg,
    playoff_weeks_avg,
    ROUND(playoff_weeks_avg - season_avg, 1) as playoff_improvement,
    ROUND((playoff_weeks_avg - season_avg) / season_avg * 100, 1) as improvement_pct
FROM recent_performance
WHERE playoff_weeks_avg > 10  -- Minimum threshold for relevance
ORDER BY improvement_pct DESC
LIMIT 20
```

<DataTable 
    data={fantasy_playoffs}
    search=true
    title="Fantasy Playoff Performance vs Season Average"
    subtitle="Players trending up in crucial weeks 14+"
/>

---

## Waiver Wire Gems

```sql waiver_wire_gems
WITH ownership_proxy AS (
    SELECT 
        player_name,
        position,
        team,
        ROUND(SUM(fantasy_points_ppr), 1) as total_points,
        ROUND(AVG(fantasy_points_ppr), 1) as avg_points,
        COUNT(*) as games_played,
        -- Proxy for low ownership: players with fewer than 8 games but high recent performance
        CASE WHEN COUNT(*) <= 8 THEN 'Likely Available' ELSE 'Likely Rostered' END as roster_status
    FROM int_player_weekly_stats
    WHERE season = 2023 AND fantasy_points_ppr > 0
    GROUP BY player_name, position, team
    HAVING COUNT(*) >= 4  -- At least 4 games to establish pattern
)
SELECT 
    player_name,
    position,
    team,
    total_points,
    avg_points,
    games_played,
    roster_status
FROM ownership_proxy
WHERE avg_points >= 12  -- Strong per-game average
    AND roster_status = 'Likely Available'
ORDER BY avg_points DESC
LIMIT 15
```

<DataTable 
    data={waiver_wire_gems}
    title="Potential Waiver Wire Targets"
    subtitle="High-scoring players with limited games (likely available)"
/>

---

## Red Zone Efficiency

```sql red_zone_efficiency
SELECT 
    player_name,
    position,
    team,
    SUM(receiving_touchdowns + rushing_touchdowns) as total_touchdowns,
    ROUND(SUM(fantasy_points_ppr), 1) as total_fantasy_points,
    COUNT(*) as games_played,
    ROUND(SUM(receiving_touchdowns + rushing_touchdowns)::FLOAT / COUNT(*), 1) as tds_per_game,
    -- TD dependency calculation
    ROUND(SUM(receiving_touchdowns + rushing_touchdowns) * 6.0 / SUM(fantasy_points_ppr) * 100, 1) as td_dependency_pct
FROM int_player_weekly_stats
WHERE season = 2023 
    AND fantasy_points_ppr > 0
    AND position IN ('RB', 'WR', 'TE')
GROUP BY player_name, position, team
HAVING COUNT(*) >= 8
    AND SUM(receiving_touchdowns + rushing_touchdowns) >= 5
ORDER BY tds_per_game DESC
LIMIT 20
```

<ScatterChart 
    data={red_zone_efficiency}
    x="tds_per_game"
    y="total_fantasy_points"
    series="position"
    size="games_played"
    title="Touchdown Production vs Total Fantasy Points"
    xAxisTitle="Touchdowns per Game"
    yAxisTitle="Total Fantasy Points"
    subtitle="Higher TD/game = more red zone opportunities"
/>

---

## Snap Count Correlation

```sql usage_correlation
WITH usage_stats AS (
    SELECT 
        player_name,
        position,
        team,
        AVG(fantasy_points_ppr) as avg_fantasy_points,
        COUNT(*) as games_played,
        SUM(targets) as total_targets,
        SUM(rushing_attempts) as total_carries,
        -- Proxy for snap share using touches
        ROUND((SUM(targets) + SUM(rushing_attempts))::FLOAT / COUNT(*), 1) as touches_per_game
    FROM int_player_weekly_stats
    WHERE season = 2023 
        AND fantasy_points_ppr > 0
        AND position IN ('RB', 'WR', 'TE')
    GROUP BY player_name, position, team
    HAVING COUNT(*) >= 8
)
SELECT 
    player_name,
    position,
    team,
    ROUND(avg_fantasy_points, 1) as avg_fantasy_points,
    touches_per_game,
    total_targets,
    total_carries
FROM usage_stats
WHERE touches_per_game >= 10  -- High usage threshold
ORDER BY avg_fantasy_points DESC
LIMIT 25
```

<ScatterChart 
    data={usage_correlation}
    x="touches_per_game"
    y="avg_fantasy_points"
    series="position"
    size="total_targets"
    title="Usage vs Fantasy Production"
    xAxisTitle="Touches per Game"
    yAxisTitle="Average Fantasy Points"
    subtitle="Higher usage typically correlates with more fantasy points"
/>

---

## Key Fantasy Insights

### Consistency Champions
- **Low Variance Leaders**: Players with best consistency scores provide reliable weekly floors
- **Target Hogs**: High target share players offer stable PPR production
- **Workhorse Backs**: RBs with 15+ touches per game typically deliver consistent fantasy value

### Efficiency Standouts
- **Points per Target**: Most efficient fantasy producers per opportunity
- **Red Zone Specialists**: High TD rates indicate goal-line priority
- **Explosive Play Ability**: Players with high ceiling games for tournaments

### Strategic Considerations
- **Playoff Schedule**: Target players on teams with favorable late-season matchups
- **Usage Trends**: Identify players with increasing snap/target share
- **Matchup Exploitation**: Focus on players facing weak positional defenses

### Roster Construction Tips
- **Balanced Approach**: Mix high-floor consistent players with high-ceiling options
- **Handcuff Strategy**: Own backup RBs for injury-prone starters
- **Streaming Positions**: Use matchup-based approach for TE and flex positions