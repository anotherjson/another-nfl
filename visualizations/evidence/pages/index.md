# NFL Analytics Dashboard

Welcome to the comprehensive NFL analytics dashboard powered by Evidence and dbt.

## Overview

This dashboard provides deep insights into NFL performance data, including:

- **Team Performance**: EPA analysis, rankings, and trends
- **Player Statistics**: Individual performance metrics and fantasy analysis
- **Game Analysis**: Historical results and betting intelligence
- **Advanced Metrics**: Next Gen Stats and situational performance

---

```sql teams_summary
SELECT 
    COUNT(DISTINCT team) as total_teams,
    COUNT(DISTINCT week) as weeks_analyzed,
    ROUND(AVG(total_epa), 3) as avg_epa,
    MAX(week) as latest_week
FROM mart_weekly_team_stats
WHERE season = 2023
```

<div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
    <div class="bg-blue-50 p-6 rounded-lg">
        <h3 class="text-2xl font-bold text-blue-600">{teams_summary[0].total_teams}</h3>
        <p class="text-gray-600">NFL Teams</p>
    </div>
    <div class="bg-green-50 p-6 rounded-lg">
        <h3 class="text-2xl font-bold text-green-600">{teams_summary[0].weeks_analyzed}</h3>
        <p class="text-gray-600">Weeks Analyzed</p>
    </div>
    <div class="bg-orange-50 p-6 rounded-lg">
        <h3 class="text-2xl font-bold text-orange-600">{teams_summary[0].avg_epa}</h3>
        <p class="text-gray-600">Average EPA</p>
    </div>
    <div class="bg-purple-50 p-6 rounded-lg">
        <h3 class="text-2xl font-bold text-purple-600">{teams_summary[0].latest_week}</h3>
        <p class="text-gray-600">Latest Week</p>
    </div>
</div>

---

## Recent Performance Trends

```sql weekly_epa_trends
SELECT 
    week,
    ROUND(AVG(total_epa), 3) as avg_epa,
    ROUND(AVG(pass_epa), 3) as avg_pass_epa,
    ROUND(AVG(rush_epa), 3) as avg_rush_epa
FROM mart_weekly_team_stats
WHERE season = 2023
GROUP BY week
ORDER BY week
```

<LineChart 
    data={weekly_epa_trends}
    x="week"
    y="avg_epa"
    title="League Average EPA by Week"
    yAxisTitle="Average EPA"
/>

---

## Top Performing Teams

```sql top_teams
SELECT 
    team,
    ROUND(AVG(total_epa), 3) as avg_epa,
    SUM(wins) as total_wins,
    SUM(losses) as total_losses,
    ROUND(SUM(wins)::FLOAT / NULLIF(SUM(wins) + SUM(losses), 0) * 100, 1) as win_percentage
FROM mart_weekly_team_stats
WHERE season = 2023
GROUP BY team
ORDER BY avg_epa DESC
LIMIT 10
```

<BarChart 
    data={top_teams}
    x="team"
    y="avg_epa"
    series="avg_epa"
    title="Top 10 Teams by Average EPA"
    yAxisTitle="Average EPA"
/>

---

## Fantasy Football Leaders

```sql fantasy_leaders
SELECT 
    player_name,
    position,
    team,
    ROUND(SUM(fantasy_points_ppr), 1) as total_fantasy_points,
    COUNT(*) as games_played,
    ROUND(AVG(fantasy_points_ppr), 1) as avg_fantasy_points
FROM int_player_weekly_stats
WHERE season = 2023 AND fantasy_points_ppr > 0
GROUP BY player_name, position, team
HAVING COUNT(*) >= 8
ORDER BY total_fantasy_points DESC
LIMIT 15
```

<DataTable data={fantasy_leaders} />

---

## Quick Navigation

<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mt-8">
    <a href="/team-performance" class="block p-6 bg-white border border-gray-200 rounded-lg hover:shadow-lg transition-shadow">
        <h3 class="text-xl font-semibold mb-2">Team Performance</h3>
        <p class="text-gray-600">Analyze team EPA, rankings, and offensive efficiency</p>
    </a>
    <a href="/player-analysis" class="block p-6 bg-white border border-gray-200 rounded-lg hover:shadow-lg transition-shadow">
        <h3 class="text-xl font-semibold mb-2">Player Analysis</h3>
        <p class="text-gray-600">Individual player statistics and performance trends</p>
    </a>
    <a href="/fantasy" class="block p-6 bg-white border border-gray-200 rounded-lg hover:shadow-lg transition-shadow">
        <h3 class="text-xl font-semibold mb-2">Fantasy Football</h3>
        <p class="text-gray-600">Fantasy points, projections, and matchup analysis</p>
    </a>
    <a href="/games" class="block p-6 bg-white border border-gray-200 rounded-lg hover:shadow-lg transition-shadow">
        <h3 class="text-xl font-semibold mb-2">Game Analysis</h3>
        <p class="text-gray-600">Game results, betting trends, and weather impact</p>
    </a>
</div>