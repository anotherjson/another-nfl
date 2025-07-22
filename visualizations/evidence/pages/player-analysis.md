# Player Performance Analysis

Deep dive into individual player statistics, efficiency metrics, and performance trends across all positions.

---

## Player Overview

```sql player_overview
SELECT 
    COUNT(DISTINCT player_name) as total_players,
    COUNT(DISTINCT position) as positions,
    ROUND(AVG(fantasy_points_ppr), 2) as avg_fantasy_points,
    ROUND(SUM(fantasy_points_ppr), 1) as total_fantasy_points,
    MAX(week) as latest_week
FROM int_player_weekly_stats
WHERE season = 2023 AND fantasy_points_ppr > 0
```

<div class="grid grid-cols-1 md:grid-cols-5 gap-4 mb-8">
    <div class="bg-blue-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-blue-600">{player_overview[0].total_players}</h3>
        <p class="text-gray-600">Active Players</p>
    </div>
    <div class="bg-green-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-green-600">{player_overview[0].positions}</h3>
        <p class="text-gray-600">Positions</p>
    </div>
    <div class="bg-orange-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-orange-600">{player_overview[0].avg_fantasy_points}</h3>
        <p class="text-gray-600">Avg Fantasy PPG</p>
    </div>
    <div class="bg-purple-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-purple-600">{player_overview[0].total_fantasy_points}</h3>
        <p class="text-gray-600">Total Fantasy Points</p>
    </div>
    <div class="bg-red-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-red-600">{player_overview[0].latest_week}</h3>
        <p class="text-gray-600">Latest Week</p>
    </div>
</div>

---

## Top Performers by Position

```sql top_performers_by_position
WITH player_totals AS (
    SELECT 
        player_name,
        position,
        team,
        ROUND(SUM(fantasy_points_ppr), 1) as total_fantasy_points,
        ROUND(AVG(fantasy_points_ppr), 1) as avg_fantasy_points,
        COUNT(*) as games_played,
        ROW_NUMBER() OVER (PARTITION BY position ORDER BY SUM(fantasy_points_ppr) DESC) as position_rank
    FROM int_player_weekly_stats
    WHERE season = 2023 AND fantasy_points_ppr > 0
    GROUP BY player_name, position, team
    HAVING COUNT(*) >= 8
)
SELECT 
    position,
    player_name,
    team,
    total_fantasy_points,
    avg_fantasy_points,
    games_played,
    position_rank
FROM player_totals
WHERE position_rank <= 5
ORDER BY position, position_rank
```

<DataTable 
    data={top_performers_by_position}
    search=true
    groupBy="position"
    title="Top 5 Players by Position (Season Totals)"
/>

---

## Weekly Performance Trends

```sql weekly_position_trends
SELECT 
    week,
    position,
    ROUND(AVG(fantasy_points_ppr), 2) as avg_fantasy_points,
    ROUND(STDDEV(fantasy_points_ppr), 2) as fantasy_points_stddev,
    COUNT(*) as players_active
FROM int_player_weekly_stats
WHERE season = 2023 AND fantasy_points_ppr > 0
GROUP BY week, position
ORDER BY week, position
```

<LineChart 
    data={weekly_position_trends}
    x="week"
    y="avg_fantasy_points"
    series="position"
    title="Average Fantasy Points by Position and Week"
    yAxisTitle="Average Fantasy Points"
    subtitle="How different positions perform throughout the season"
/>

---

## Player Efficiency Metrics

### Target Efficiency (WR, TE, RB)

```sql target_efficiency
SELECT 
    player_name,
    position,
    team,
    SUM(targets) as total_targets,
    SUM(receptions) as total_receptions,
    ROUND(SUM(receptions)::FLOAT / NULLIF(SUM(targets), 0) * 100, 1) as catch_rate,
    ROUND(SUM(receiving_yards)::FLOAT / NULLIF(SUM(receptions), 0), 1) as yards_per_reception,
    ROUND(SUM(fantasy_points_ppr)::FLOAT / NULLIF(SUM(targets), 0), 2) as fantasy_points_per_target,
    ROUND(SUM(fantasy_points_ppr), 1) as total_fantasy_points
FROM int_player_weekly_stats
WHERE season = 2023 
    AND position IN ('WR', 'TE', 'RB')
    AND targets > 0
GROUP BY player_name, position, team
HAVING SUM(targets) >= 30
ORDER BY fantasy_points_per_target DESC
LIMIT 30
```

<ScatterChart 
    data={target_efficiency}
    x="catch_rate"
    y="fantasy_points_per_target"
    series="position"
    size="total_targets"
    title="Target Efficiency: Catch Rate vs Fantasy Points per Target"
    xAxisTitle="Catch Rate (%)"
    yAxisTitle="Fantasy Points per Target"
    subtitle="Bubble size represents total targets"
/>

---

## Consistency Analysis

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
LIMIT 40
```

<ScatterChart 
    data={player_consistency}
    x="consistency_score"
    y="best_game"
    series="position"
    size="avg_points"
    title="Player Consistency vs Ceiling (Bubble Size = Average Points)"
    xAxisTitle="Consistency Score (Higher = More Consistent)"
    yAxisTitle="Best Single Game Performance"
    subtitle="Players in top-right have both high floors and ceilings"
/>

---

## Position-Specific Analysis

<div class="grid grid-cols-1 md:grid-cols-2 gap-8">

### Quarterback Performance
```sql qb_performance
SELECT 
    player_name,
    team,
    ROUND(SUM(fantasy_points_ppr), 1) as total_points,
    ROUND(AVG(fantasy_points_ppr), 1) as avg_points,
    COUNT(*) as games_started,
    SUM(passing_yards) as total_passing_yards,
    SUM(passing_touchdowns) as total_passing_tds
FROM int_player_weekly_stats
WHERE season = 2023 AND position = 'QB' AND fantasy_points_ppr > 0
GROUP BY player_name, team
HAVING COUNT(*) >= 8
ORDER BY total_points DESC
LIMIT 15
```

<BarChart 
    data={qb_performance}
    x="player_name"
    y="total_points"
    title="Top 15 Quarterbacks by Total Fantasy Points"
    yAxisTitle="Total Fantasy Points"
    xAxisTitle="Player"
/>

### Running Back Workload
```sql rb_workload
SELECT 
    player_name,
    team,
    ROUND(SUM(fantasy_points_ppr), 1) as total_points,
    SUM(rushing_attempts) as total_carries,
    SUM(targets) as total_targets,
    ROUND((SUM(rushing_attempts) + SUM(targets))::FLOAT / COUNT(*), 1) as touches_per_game,
    COUNT(*) as games_played
FROM int_player_weekly_stats
WHERE season = 2023 AND position = 'RB' AND fantasy_points_ppr > 0
GROUP BY player_name, team
HAVING COUNT(*) >= 8
ORDER BY touches_per_game DESC
LIMIT 20
```

<BarChart 
    data={rb_workload}
    x="player_name"
    y="touches_per_game"
    title="RB Workload: Touches per Game"
    yAxisTitle="Touches per Game"
    xAxisTitle="Player"
/>

</div>

---

## Target Share Analysis

```sql target_share_analysis
WITH team_targets AS (
    SELECT 
        team,
        week,
        SUM(targets) as team_total_targets
    FROM int_player_weekly_stats
    WHERE season = 2023 AND position IN ('WR', 'TE', 'RB')
    GROUP BY team, week
),
player_target_share AS (
    SELECT 
        p.player_name,
        p.position,
        p.team,
        p.week,
        p.targets,
        p.fantasy_points_ppr,
        ROUND(p.targets::FLOAT / NULLIF(t.team_total_targets, 0) * 100, 1) as target_share_pct
    FROM int_player_weekly_stats p
    JOIN team_targets t ON p.team = t.team AND p.week = t.week
    WHERE p.season = 2023 AND p.position IN ('WR', 'TE', 'RB')
        AND p.targets > 0
)
SELECT 
    player_name,
    position,
    team,
    ROUND(AVG(target_share_pct), 1) as avg_target_share,
    ROUND(AVG(fantasy_points_ppr), 1) as avg_fantasy_points,
    SUM(targets) as total_targets,
    COUNT(*) as games_with_targets
FROM player_target_share
GROUP BY player_name, position, team
HAVING SUM(targets) >= 40
ORDER BY avg_target_share DESC
LIMIT 25
```

<DataTable 
    data={target_share_analysis}
    search=true
    title="Target Share Leaders (Min. 40 targets)"
    subtitle="Players with highest percentage of team targets"
/>

---

## Breakout Player Analysis

```sql breakout_candidates
WITH player_progression AS (
    SELECT 
        player_name,
        position,
        team,
        week,
        fantasy_points_ppr,
        AVG(fantasy_points_ppr) OVER (
            PARTITION BY player_name 
            ORDER BY week 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as rolling_avg_4wk
    FROM int_player_weekly_stats
    WHERE season = 2023 AND fantasy_points_ppr > 0
),
recent_vs_early AS (
    SELECT 
        player_name,
        position,
        team,
        ROUND(AVG(CASE WHEN week <= 8 THEN rolling_avg_4wk END), 2) as early_season_avg,
        ROUND(AVG(CASE WHEN week >= 10 THEN rolling_avg_4wk END), 2) as late_season_avg,
        COUNT(CASE WHEN week <= 8 THEN 1 END) as early_games,
        COUNT(CASE WHEN week >= 10 THEN 1 END) as late_games
    FROM player_progression
    WHERE rolling_avg_4wk IS NOT NULL
    GROUP BY player_name, position, team
    HAVING COUNT(CASE WHEN week <= 8 THEN 1 END) >= 4
        AND COUNT(CASE WHEN week >= 10 THEN 1 END) >= 4
)
SELECT 
    player_name,
    position,
    team,
    early_season_avg,
    late_season_avg,
    ROUND(late_season_avg - early_season_avg, 2) as improvement,
    ROUND((late_season_avg - early_season_avg) / early_season_avg * 100, 1) as improvement_pct
FROM recent_vs_early
WHERE late_season_avg > early_season_avg
    AND early_season_avg > 5  -- Minimum early season average
ORDER BY improvement_pct DESC
LIMIT 15
```

<BarChart 
    data={breakout_candidates}
    x="player_name"
    y="improvement_pct"
    series="position"
    title="Breakout Players: Late Season vs Early Season Improvement"
    yAxisTitle="Fantasy Points Improvement (%)"
    xAxisTitle="Player"
/>

---

## Key Performance Insights

### Consistency Leaders
- **Highest Consistency Scores**: Players with best ratio of average points to standard deviation
- **Low Variance, High Production**: Ideal for fantasy football and reliable performance
- **Position Differences**: QBs typically more consistent than skill position players

### Efficiency Metrics
- **Target Share**: Players commanding higher percentage of team targets
- **Points per Target**: Most efficient fantasy producers per opportunity
- **Workload Correlation**: Higher touch players generally score more fantasy points

### Breakout Analysis
- **In-Season Improvement**: Players showing significant late-season improvement
- **Usage Trends**: Increasing target share and opportunity allocation
- **Situational Factors**: Injuries, scheme changes, and role expansion