# Team Performance Analysis

Comprehensive analysis of NFL team performance using Expected Points Added (EPA) and other advanced metrics.

---

## Executive Summary

```sql team_overview
SELECT 
    COUNT(DISTINCT team) as total_teams,
    COUNT(DISTINCT week) as weeks_analyzed,
    ROUND(AVG(total_epa), 3) as league_avg_epa,
    ROUND(MAX(total_epa) - MIN(total_epa), 3) as epa_range,
    MAX(week) as latest_week
FROM mart_weekly_team_stats
WHERE season = 2023
```

<div class="grid grid-cols-1 md:grid-cols-5 gap-4 mb-8">
    <div class="bg-blue-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-blue-600">{team_overview[0].total_teams}</h3>
        <p class="text-gray-600">NFL Teams</p>
    </div>
    <div class="bg-green-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-green-600">{team_overview[0].weeks_analyzed}</h3>
        <p class="text-gray-600">Weeks Analyzed</p>
    </div>
    <div class="bg-orange-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-orange-600">{team_overview[0].league_avg_epa}</h3>
        <p class="text-gray-600">League Avg EPA</p>
    </div>
    <div class="bg-purple-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-purple-600">{team_overview[0].epa_range}</h3>
        <p class="text-gray-600">EPA Range</p>
    </div>
    <div class="bg-red-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-red-600">{team_overview[0].latest_week}</h3>
        <p class="text-gray-600">Latest Week</p>
    </div>
</div>

---

## EPA Rankings

```sql team_rankings
SELECT 
    team,
    ROUND(AVG(total_epa), 3) as avg_total_epa,
    ROUND(AVG(pass_epa), 3) as avg_pass_epa,
    ROUND(AVG(rush_epa), 3) as avg_rush_epa,
    SUM(wins) as wins,
    SUM(losses) as losses,
    ROUND(AVG(total_epa) OVER (), 3) as league_avg_epa,
    ROW_NUMBER() OVER (ORDER BY AVG(total_epa) DESC) as epa_rank
FROM mart_weekly_team_stats
WHERE season = 2023
GROUP BY team
ORDER BY avg_total_epa DESC
```

<DataTable 
    data={team_rankings}
    search=true
    totalRows=true
/>

---

## EPA Trends Over Time

```sql weekly_epa_by_team
SELECT 
    week,
    team,
    total_epa,
    pass_epa,
    rush_epa
FROM mart_weekly_team_stats
WHERE season = 2023
ORDER BY week, total_epa DESC
```

<LineChart 
    data={weekly_epa_by_team}
    x="week"
    y="total_epa"
    series="team"
    title="Total EPA by Week (All Teams)"
    yAxisTitle="Total EPA"
/>

---

## Offensive Efficiency Breakdown

### Pass EPA vs Rush EPA

```sql epa_breakdown
SELECT 
    team,
    ROUND(AVG(pass_epa), 3) as avg_pass_epa,
    ROUND(AVG(rush_epa), 3) as avg_rush_epa,
    ROUND(AVG(total_epa), 3) as avg_total_epa
FROM mart_weekly_team_stats
WHERE season = 2023
GROUP BY team
ORDER BY avg_total_epa DESC
```

<ScatterChart 
    data={epa_breakdown}
    x="avg_pass_epa"
    y="avg_rush_epa"
    series="team"
    size="avg_total_epa"
    title="Pass EPA vs Rush EPA (Season Averages)"
    xAxisTitle="Average Pass EPA"
    yAxisTitle="Average Rush EPA"
/>

---

## Weekly Performance Heatmap

```sql weekly_rankings
SELECT 
    team,
    week,
    total_epa,
    ROW_NUMBER() OVER (PARTITION BY week ORDER BY total_epa DESC) as weekly_rank
FROM mart_weekly_team_stats
WHERE season = 2023
ORDER BY team, week
```

<Heatmap 
    data={weekly_rankings}
    x="week"
    y="team"
    value="weekly_rank"
    title="Weekly EPA Rankings (1=Best, 32=Worst)"
    colorScale="red_yellow_green_r"
/>

---

## Top and Bottom Performers

<div class="grid grid-cols-1 md:grid-cols-2 gap-8">

### Top 5 EPA Teams
```sql top_5_teams
SELECT 
    team,
    ROUND(AVG(total_epa), 3) as avg_epa,
    SUM(wins) as wins,
    SUM(losses) as losses
FROM mart_weekly_team_stats
WHERE season = 2023
GROUP BY team
ORDER BY avg_epa DESC
LIMIT 5
```

<BarChart 
    data={top_5_teams}
    x="team"
    y="avg_epa"
    title="Top 5 Teams by EPA"
    yAxisTitle="Average EPA"
/>

### Bottom 5 EPA Teams
```sql bottom_5_teams
SELECT 
    team,
    ROUND(AVG(total_epa), 3) as avg_epa,
    SUM(wins) as wins,
    SUM(losses) as losses
FROM mart_weekly_team_stats
WHERE season = 2023
GROUP BY team
ORDER BY avg_epa ASC
LIMIT 5
```

<BarChart 
    data={bottom_5_teams}
    x="team"
    y="avg_epa"
    title="Bottom 5 Teams by EPA"
    yAxisTitle="Average EPA"
/>

</div>

---

## Consistency Analysis

```sql team_consistency
SELECT 
    team,
    ROUND(AVG(total_epa), 3) as avg_epa,
    ROUND(STDDEV(total_epa), 3) as epa_stddev,
    ROUND(MAX(total_epa), 3) as best_epa,
    ROUND(MIN(total_epa), 3) as worst_epa,
    COUNT(*) as games_played
FROM mart_weekly_team_stats
WHERE season = 2023
GROUP BY team
ORDER BY epa_stddev ASC
```

<ScatterChart 
    data={team_consistency}
    x="avg_epa"
    y="epa_stddev"
    series="team"
    title="EPA Consistency vs Average (Lower Standard Deviation = More Consistent)"
    xAxisTitle="Average EPA"
    yAxisTitle="EPA Standard Deviation"
/>