# Game Analysis & Betting Intelligence

Comprehensive analysis of game outcomes, betting lines, weather impacts, and predictive modeling for NFL games.

---

## Game Summary

```sql games_overview
SELECT 
    COUNT(*) as total_games,
    COUNT(DISTINCT week) as weeks_played,
    ROUND(AVG(home_score + away_score), 1) as avg_total_score,
    ROUND(AVG(home_score), 1) as avg_home_score,
    ROUND(AVG(away_score), 1) as avg_away_score,
    COUNT(CASE WHEN home_score > away_score THEN 1 END) as home_wins,
    ROUND(COUNT(CASE WHEN home_score > away_score THEN 1 END)::FLOAT / COUNT(*) * 100, 1) as home_win_rate
FROM mart_game_results
WHERE season = 2023
```

<div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
    <div class="bg-blue-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-blue-600">{games_overview[0].total_games}</h3>
        <p class="text-gray-600">Total Games</p>
    </div>
    <div class="bg-green-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-green-600">{games_overview[0].avg_total_score}</h3>
        <p class="text-gray-600">Avg Total Score</p>
    </div>
    <div class="bg-orange-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-orange-600">{games_overview[0].home_win_rate}%</h3>
        <p class="text-gray-600">Home Win Rate</p>
    </div>
    <div class="bg-purple-50 p-6 rounded-lg text-center">
        <h3 class="text-3xl font-bold text-purple-600">{games_overview[0].weeks_played}</h3>
        <p class="text-gray-600">Weeks Played</p>
    </div>
</div>

---

## Scoring Trends by Week

```sql weekly_scoring_trends
SELECT 
    week,
    ROUND(AVG(home_score + away_score), 1) as avg_total_points,
    ROUND(AVG(home_score), 1) as avg_home_score,
    ROUND(AVG(away_score), 1) as avg_away_score,
    COUNT(*) as games_played,
    ROUND(STDDEV(home_score + away_score), 1) as scoring_variance
FROM mart_game_results
WHERE season = 2023
GROUP BY week
ORDER BY week
```

<LineChart 
    data={weekly_scoring_trends}
    x="week"
    y={["avg_total_points", "avg_home_score", "avg_away_score"]}
    title="Scoring Trends Throughout the Season"
    yAxisTitle="Average Points"
    subtitle="How scoring patterns change week by week"
/>

---

## Home Field Advantage Analysis

```sql home_field_advantage
SELECT 
    home_team,
    COUNT(*) as home_games,
    COUNT(CASE WHEN home_score > away_score THEN 1 END) as home_wins,
    ROUND(COUNT(CASE WHEN home_score > away_score THEN 1 END)::FLOAT / COUNT(*) * 100, 1) as home_win_pct,
    ROUND(AVG(home_score - away_score), 1) as avg_point_differential,
    ROUND(AVG(home_score), 1) as avg_home_score,
    ROUND(AVG(away_score), 1) as avg_away_score_against
FROM mart_game_results
WHERE season = 2023
GROUP BY home_team
ORDER BY home_win_pct DESC
```

<BarChart 
    data={home_field_advantage}
    x="home_team"
    y="home_win_pct"
    title="Home Win Percentage by Team"
    yAxisTitle="Home Win Percentage (%)"
    subtitle="Which teams have the strongest home field advantage"
/>

---

## Betting Lines Analysis

```sql betting_performance
SELECT 
    home_team,
    away_team,
    week,
    home_score,
    away_score,
    spread_line,
    total_line,
    (home_score - away_score) as actual_spread,
    (home_score + away_score) as actual_total,
    CASE 
        WHEN (home_score - away_score) > spread_line THEN 'Home Cover'
        WHEN (home_score - away_score) < spread_line THEN 'Away Cover'
        ELSE 'Push'
    END as spread_result,
    CASE 
        WHEN (home_score + away_score) > total_line THEN 'Over'
        WHEN (home_score + away_score) < total_line THEN 'Under'
        ELSE 'Push'
    END as total_result
FROM mart_game_results
WHERE season = 2023 
    AND spread_line IS NOT NULL 
    AND total_line IS NOT NULL
ORDER BY week, home_team
```

### Spread Performance
```sql spread_summary
SELECT 
    spread_result,
    COUNT(*) as games,
    ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER () * 100, 1) as percentage
FROM (
    SELECT 
        CASE 
            WHEN (home_score - away_score) > spread_line THEN 'Home Cover'
            WHEN (home_score - away_score) < spread_line THEN 'Away Cover'
            ELSE 'Push'
        END as spread_result
    FROM mart_game_results
    WHERE season = 2023 AND spread_line IS NOT NULL
) spread_data
GROUP BY spread_result
ORDER BY games DESC
```

<div class="grid grid-cols-1 md:grid-cols-2 gap-8">

<BarChart 
    data={spread_summary}
    x="spread_result"
    y="percentage"
    title="Spread Betting Results"
    yAxisTitle="Percentage of Games (%)"
/>

### Over/Under Performance
```sql total_summary
SELECT 
    total_result,
    COUNT(*) as games,
    ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER () * 100, 1) as percentage
FROM (
    SELECT 
        CASE 
            WHEN (home_score + away_score) > total_line THEN 'Over'
            WHEN (home_score + away_score) < total_line THEN 'Under'
            ELSE 'Push'
        END as total_result
    FROM mart_game_results
    WHERE season = 2023 AND total_line IS NOT NULL
) total_data
GROUP BY total_result
ORDER BY games DESC
```

<BarChart 
    data={total_summary}
    x="total_result"
    y="percentage"
    title="Over/Under Betting Results"
    yAxisTitle="Percentage of Games (%)"
/>

</div>

---

## Weather Impact Analysis

```sql weather_impact
SELECT 
    CASE 
        WHEN weather_temperature IS NULL THEN 'Dome/Unknown'
        WHEN weather_temperature < 32 THEN 'Freezing (< 32°F)'
        WHEN weather_temperature BETWEEN 32 AND 50 THEN 'Cold (32-50°F)'
        WHEN weather_temperature BETWEEN 51 AND 70 THEN 'Mild (51-70°F)'
        ELSE 'Warm (> 70°F)'
    END as temp_category,
    COUNT(*) as games,
    ROUND(AVG(home_score + away_score), 1) as avg_total_score,
    ROUND(AVG(CASE WHEN home_score > away_score THEN 100.0 ELSE 0.0 END), 1) as home_win_rate,
    ROUND(AVG(weather_temperature), 1) as avg_temperature
FROM mart_game_results
WHERE season = 2023
GROUP BY temp_category
ORDER BY avg_temperature NULLS FIRST
```

<BarChart 
    data={weather_impact}
    x="temp_category"
    y="avg_total_score"
    title="Scoring by Weather Conditions"
    yAxisTitle="Average Total Score"
    subtitle="How temperature affects game scoring"
/>

---

## Wind Impact on Scoring

```sql wind_impact
SELECT 
    CASE 
        WHEN weather_wind_mph IS NULL THEN 'Unknown'
        WHEN weather_wind_mph <= 5 THEN 'Calm (≤ 5 mph)'
        WHEN weather_wind_mph BETWEEN 6 AND 15 THEN 'Moderate (6-15 mph)'
        ELSE 'Windy (> 15 mph)'
    END as wind_category,
    COUNT(*) as games,
    ROUND(AVG(home_score + away_score), 1) as avg_total_score,
    ROUND(AVG(weather_wind_mph), 1) as avg_wind_speed,
    ROUND(STDDEV(home_score + away_score), 1) as score_variance
FROM mart_game_results
WHERE season = 2023
GROUP BY wind_category
ORDER BY avg_wind_speed NULLS FIRST
```

<ScatterChart 
    data={wind_impact}
    x="avg_wind_speed"
    y="avg_total_score"
    size="games"
    title="Wind Speed vs Average Scoring"
    xAxisTitle="Average Wind Speed (mph)"
    yAxisTitle="Average Total Score"
    subtitle="Bubble size represents number of games"
/>

---

## High-Scoring vs Low-Scoring Games

```sql scoring_distribution
WITH game_categories AS (
    SELECT 
        home_team,
        away_team,
        week,
        (home_score + away_score) as total_score,
        CASE 
            WHEN (home_score + away_score) >= 55 THEN 'High-Scoring (55+)'
            WHEN (home_score + away_score) BETWEEN 45 AND 54 THEN 'Above Average (45-54)'
            WHEN (home_score + away_score) BETWEEN 35 AND 44 THEN 'Average (35-44)'
            ELSE 'Low-Scoring (< 35)'
        END as scoring_category
    FROM mart_game_results
    WHERE season = 2023
)
SELECT 
    scoring_category,
    COUNT(*) as games,
    ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER () * 100, 1) as percentage,
    ROUND(AVG(total_score), 1) as avg_score_in_category,
    ROUND(MIN(total_score), 1) as min_score,
    ROUND(MAX(total_score), 1) as max_score
FROM game_categories
GROUP BY scoring_category
ORDER BY avg_score_in_category DESC
```

<DataTable 
    data={scoring_distribution}
    title="Game Scoring Distribution"
    subtitle="Breakdown of games by total scoring categories"
/>

---

## Divisional vs Non-Divisional Games

```sql divisional_analysis
WITH team_divisions AS (
    SELECT DISTINCT team,
        CASE 
            WHEN team IN ('BUF', 'MIA', 'NYJ', 'NE') THEN 'AFC East'
            WHEN team IN ('BAL', 'CIN', 'CLE', 'PIT') THEN 'AFC North'
            WHEN team IN ('HOU', 'IND', 'JAX', 'TEN') THEN 'AFC South'
            WHEN team IN ('DEN', 'KC', 'LV', 'LAC') THEN 'AFC West'
            WHEN team IN ('DAL', 'NYG', 'PHI', 'WAS') THEN 'NFC East'
            WHEN team IN ('CHI', 'DET', 'GB', 'MIN') THEN 'NFC North'
            WHEN team IN ('ATL', 'CAR', 'NO', 'TB') THEN 'NFC South'
            WHEN team IN ('ARI', 'LA', 'SF', 'SEA') THEN 'NFC West'
        END as division
    FROM mart_game_results
    WHERE season = 2023
),
divisional_games AS (
    SELECT 
        g.*,
        hd.division as home_division,
        ad.division as away_division,
        CASE WHEN hd.division = ad.division THEN 'Divisional' ELSE 'Non-Divisional' END as game_type
    FROM mart_game_results g
    JOIN team_divisions hd ON g.home_team = hd.team
    JOIN team_divisions ad ON g.away_team = ad.team
    WHERE g.season = 2023
)
SELECT 
    game_type,
    COUNT(*) as games,
    ROUND(AVG(home_score + away_score), 1) as avg_total_score,
    ROUND(AVG(ABS(home_score - away_score)), 1) as avg_margin,
    COUNT(CASE WHEN ABS(home_score - away_score) <= 3 THEN 1 END) as close_games,
    ROUND(COUNT(CASE WHEN ABS(home_score - away_score) <= 3 THEN 1 END)::FLOAT / COUNT(*) * 100, 1) as close_game_pct
FROM divisional_games
GROUP BY game_type
ORDER BY avg_total_score DESC
```

<DataTable 
    data={divisional_analysis}
    title="Divisional vs Non-Divisional Game Analysis"
    subtitle="Comparing competitiveness and scoring between division rivals and other matchups"
/>

---

## Prime Time Game Performance

```sql primetime_analysis
SELECT 
    week,
    home_team,
    away_team,
    home_score,
    away_score,
    (home_score + away_score) as total_score,
    ABS(home_score - away_score) as margin,
    CASE 
        WHEN week = 1 AND home_team IN ('KC', 'DET') THEN 'Thursday Night Football'
        WHEN home_team IN ('DAL', 'GB', 'DET') THEN 'Thanksgiving'
        ELSE 'Prime Time'
    END as game_designation
FROM mart_game_results
WHERE season = 2023
    AND (
        week = 1 OR  -- Assume opening night
        home_team IN ('DAL', 'GB', 'DET')  -- Thanksgiving games
    )
ORDER BY week, total_score DESC
```

<DataTable 
    data={primetime_analysis}
    title="Prime Time & Holiday Game Results"
    subtitle="Performance in marquee games and national television appearances"
/>

---

## Key Betting & Game Insights

### Betting Trends
- **Spread Coverage**: Track which side (home/away) covers more frequently
- **Over/Under Performance**: Analyze total scoring vs betting lines
- **Weather Factors**: Cold and windy conditions typically reduce scoring

### Home Field Advantage
- **League Average**: ~55-57% home win rate is typical
- **Team Variations**: Some teams have much stronger home advantages
- **Scoring Impact**: Home teams average 1-3 points more than visitors

### Weather Effects
- **Temperature**: Games below freezing show reduced total scoring
- **Wind**: High wind (15+ mph) significantly impacts passing games
- **Precipitation**: Rain and snow reduce offensive efficiency

### Game Flow Analysis
- **Divisional Games**: Typically closer margins and more competitive
- **Prime Time**: Often feature higher-scoring, more exciting games
- **Late Season**: Weather becomes more significant factor in northern cities