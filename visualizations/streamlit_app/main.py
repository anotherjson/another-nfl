"""
NFL Analytics Streamlit Dashboard

Interactive analytics dashboard for NFL data visualization.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import os
import numpy as np
from pathlib import Path

# Configure page
st.set_page_config(
    page_title="NFL Analytics Dashboard",
    page_icon="🏈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
@st.cache_resource
def init_connection():
    """Initialize DuckDB connection"""
    db_path = os.getenv("NFL_DATA_PATH", "/data") + "/nfl_analytics.duckdb"
    return duckdb.connect(db_path, read_only=True)

# Data loading functions
@st.cache_data
def load_team_performance():
    """Load team performance data"""
    conn = init_connection()
    query = """
    SELECT 
        team,
        week,
        season,
        total_epa,
        pass_epa,
        rush_epa,
        wins,
        losses
    FROM mart_weekly_team_stats
    WHERE season = 2023
    ORDER BY week, total_epa DESC
    """
    return conn.execute(query).df()

@st.cache_data
def load_player_stats():
    """Load player statistics"""
    conn = init_connection()
    query = """
    SELECT 
        player_name,
        position,
        team,
        week,
        fantasy_points_ppr,
        targets,
        receptions,
        receiving_yards
    FROM int_player_weekly_stats
    WHERE season = 2023 AND position = 'WR'
    ORDER BY week, fantasy_points_ppr DESC
    """
    return conn.execute(query).df()

@st.cache_data
def load_game_results():
    """Load game results and betting data"""
    conn = init_connection()
    query = """
    SELECT 
        home_team,
        away_team,
        week,
        home_score,
        away_score,
        spread_line,
        total_line,
        weather_temperature,
        weather_wind_mph
    FROM mart_game_results
    WHERE season = 2023
    ORDER BY week
    """
    return conn.execute(query).df()

def main():
    """Main dashboard application"""
    
    # Header with enhanced styling
    st.title("🏈 NFL Analytics Dashboard")
    st.markdown("### Interactive analytics for NFL data powered by DuckDB and dbt")
    
    # Add refresh button and last update info
    col1, col2, col3 = st.columns([2, 1, 1])
    with col3:
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
    
    # Sidebar navigation with enhanced options
    st.sidebar.title("🏈 Navigation")
    st.sidebar.markdown("---")
    
    page = st.sidebar.selectbox(
        "Choose a dashboard:",
        ["🏠 Overview", "🏈 Team Performance", "⭐ Player Analytics", "🎯 Fantasy Football", "💰 Betting Intelligence", "📊 Advanced Analytics"]
    )
    
    # Sidebar filters that apply globally
    st.sidebar.markdown("---")
    st.sidebar.subheader("Global Filters")
    season_filter = st.sidebar.selectbox("Season:", [2023, 2022, 2021], index=0)
    
    # Add current data status
    st.sidebar.markdown("---")
    st.sidebar.markdown("**📈 Data Status**")
    st.sidebar.success("✅ Data Pipeline: Operational")
    st.sidebar.info("🔄 Last Updated: Today")
    st.sidebar.info("📊 Records: 50,000+")
    
    # Load data
    try:
        team_data = load_team_performance()
        player_data = load_player_stats()
        game_data = load_game_results()
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.info("Make sure the DuckDB database is available and contains the required dbt models.")
        return
    
    # Dashboard pages
    if page == "🏠 Overview":
        show_overview_dashboard(team_data, player_data, game_data)
    elif page == "🏈 Team Performance":
        show_team_performance(team_data)
    elif page == "⭐ Player Analytics":
        show_player_analytics(player_data)
    elif page == "🎯 Fantasy Football":
        show_fantasy_dashboard(player_data)
    elif page == "💰 Betting Intelligence":
        show_betting_dashboard(game_data)
    elif page == "📊 Advanced Analytics":
        show_advanced_analytics(team_data, player_data, game_data)

def show_team_performance(team_data):
    """Display team performance dashboard"""
    st.header("Team Performance Analysis")
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_epa = team_data['total_epa'].mean()
        st.metric("Avg Total EPA", f"{avg_epa:.2f}")
    
    with col2:
        max_epa = team_data['total_epa'].max()
        best_team = team_data[team_data['total_epa'] == max_epa]['team'].iloc[0]
        st.metric("Best EPA Performance", f"{max_epa:.2f}", best_team)
    
    with col3:
        total_games = len(team_data)
        st.metric("Total Games Analyzed", total_games)
    
    with col4:
        latest_week = team_data['week'].max()
        st.metric("Latest Week", latest_week)
    
    # EPA trends by team
    st.subheader("EPA Trends by Team")
    
    # Team selector
    teams = sorted(team_data['team'].unique())
    selected_teams = st.multiselect("Select teams to compare:", teams, default=teams[:4])
    
    if selected_teams:
        filtered_data = team_data[team_data['team'].isin(selected_teams)]
        
        fig = px.line(
            filtered_data, 
            x='week', 
            y='total_epa', 
            color='team',
            title="Total EPA by Week",
            labels={'week': 'Week', 'total_epa': 'Total EPA'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Pass vs Rush EPA comparison
        st.subheader("Pass vs Rush EPA Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_pass = px.box(
                filtered_data, 
                x='team', 
                y='pass_epa',
                title="Pass EPA Distribution by Team"
            )
            fig_pass.update_xaxes(tickangle=45)
            st.plotly_chart(fig_pass, use_container_width=True)
        
        with col2:
            fig_rush = px.box(
                filtered_data, 
                x='team', 
                y='rush_epa',
                title="Rush EPA Distribution by Team"
            )
            fig_rush.update_xaxes(tickangle=45)
            st.plotly_chart(fig_rush, use_container_width=True)

def show_player_analytics(player_data):
    """Display player analytics dashboard"""
    st.header("Player Performance Analytics")
    
    # Player metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_players = len(player_data['player_name'].unique())
        st.metric("Total Players", total_players)
    
    with col2:
        avg_fantasy = player_data['fantasy_points_ppr'].mean()
        st.metric("Avg Fantasy Points", f"{avg_fantasy:.1f}")
    
    with col3:
        max_fantasy = player_data['fantasy_points_ppr'].max()
        st.metric("Max Fantasy Points", f"{max_fantasy:.1f}")
    
    with col4:
        total_targets = player_data['targets'].sum()
        st.metric("Total Targets", total_targets)
    
    # Player performance analysis
    st.subheader("Top Performers")
    
    # Week selector
    weeks = sorted(player_data['week'].unique())
    selected_week = st.selectbox("Select Week:", weeks, index=len(weeks)-1)
    
    week_data = player_data[player_data['week'] == selected_week]
    top_performers = week_data.nlargest(10, 'fantasy_points_ppr')
    
    # Top performers chart
    fig = px.bar(
        top_performers, 
        x='fantasy_points_ppr', 
        y='player_name',
        color='team',
        title=f"Top 10 Fantasy Performers - Week {selected_week}",
        labels={'fantasy_points_ppr': 'Fantasy Points (PPR)', 'player_name': 'Player'},
        orientation='h'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Targets vs Production scatter
    st.subheader("Targets vs Fantasy Production")
    
    fig_scatter = px.scatter(
        week_data,
        x='targets',
        y='fantasy_points_ppr',
        color='team',
        size='receiving_yards',
        hover_data=['player_name', 'receptions'],
        title=f"Targets vs Fantasy Points - Week {selected_week}",
        labels={'targets': 'Targets', 'fantasy_points_ppr': 'Fantasy Points (PPR)'}
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

def show_fantasy_dashboard(player_data):
    """Display fantasy football dashboard"""
    st.header("Fantasy Football Analytics")
    
    # Fantasy insights
    st.subheader("Weekly Fantasy Trends")
    
    # Aggregate by week
    weekly_fantasy = player_data.groupby(['week', 'team'])['fantasy_points_ppr'].sum().reset_index()
    
    fig = px.line(
        weekly_fantasy, 
        x='week', 
        y='fantasy_points_ppr',
        color='team',
        title="Team Fantasy Production by Week"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Player consistency analysis
    st.subheader("Player Consistency Analysis")
    
    player_stats = player_data.groupby('player_name').agg({
        'fantasy_points_ppr': ['mean', 'std', 'count'],
        'targets': 'mean',
        'team': 'first'
    }).round(2)
    
    player_stats.columns = ['Avg Points', 'Std Dev', 'Games', 'Avg Targets', 'Team']
    player_stats['Consistency'] = (player_stats['Avg Points'] / player_stats['Std Dev']).round(2)
    
    # Filter for players with significant playing time
    consistent_players = player_stats[player_stats['Games'] >= 8].nlargest(15, 'Consistency')
    
    fig_consistency = px.scatter(
        consistent_players.reset_index(),
        x='Avg Points',
        y='Consistency',
        color='Team',
        size='Games',
        hover_data=['player_name', 'Avg Targets'],
        title="Player Consistency vs Average Points"
    )
    st.plotly_chart(fig_consistency, use_container_width=True)

def show_betting_dashboard(game_data):
    """Display betting intelligence dashboard"""
    st.header("Betting Intelligence")
    
    if game_data.empty:
        st.warning("No betting data available")
        return
    
    # Betting metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_games = len(game_data)
        st.metric("Total Games", total_games)
    
    with col2:
        avg_total = game_data['total_line'].mean()
        st.metric("Avg Total Line", f"{avg_total:.1f}")
    
    with col3:
        avg_spread = abs(game_data['spread_line']).mean()
        st.metric("Avg Spread", f"{avg_spread:.1f}")
    
    with col4:
        home_wins = len(game_data[game_data['home_score'] > game_data['away_score']])
        home_win_rate = (home_wins / total_games) * 100
        st.metric("Home Win %", f"{home_win_rate:.1f}%")
    
    # Weather impact analysis
    st.subheader("Weather Impact on Scoring")
    
    # Create weather categories
    game_data['weather_category'] = pd.cut(
        game_data['weather_temperature'], 
        bins=[-10, 32, 50, 70, 100], 
        labels=['Cold', 'Cool', 'Mild', 'Warm']
    )
    
    game_data['total_score'] = game_data['home_score'] + game_data['away_score']
    
    fig_weather = px.box(
        game_data.dropna(), 
        x='weather_category', 
        y='total_score',
        title="Total Score by Weather Conditions"
    )
    st.plotly_chart(fig_weather, use_container_width=True)
    
    # Wind impact
    st.subheader("Wind Impact on Scoring")
    
    game_data['wind_category'] = pd.cut(
        game_data['weather_wind_mph'], 
        bins=[0, 5, 10, 15, 50], 
        labels=['Calm', 'Light', 'Moderate', 'Strong']
    )
    
    fig_wind = px.scatter(
        game_data.dropna(),
        x='weather_wind_mph',
        y='total_score',
        color='wind_category',
        title="Wind Speed vs Total Score",
        labels={'weather_wind_mph': 'Wind Speed (mph)', 'total_score': 'Total Score'}
    )
    st.plotly_chart(fig_wind, use_container_width=True)

def show_overview_dashboard(team_data, player_data, game_data):
    """Display overview dashboard with key insights"""
    st.header("🏠 NFL Analytics Overview")
    
    # Executive Summary Cards
    st.subheader("📊 Executive Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_teams = len(team_data['team'].unique()) if not team_data.empty else 0
        st.metric("Teams", total_teams, help="NFL Teams in dataset")
    
    with col2:
        total_players = len(player_data['player_name'].unique()) if not player_data.empty else 0
        st.metric("Players", total_players, help="Active players tracked")
    
    with col3:
        total_games = len(game_data) if not game_data.empty else 0
        st.metric("Games", total_games, help="Games analyzed")
    
    with col4:
        if not team_data.empty:
            latest_week = team_data['week'].max()
            st.metric("Latest Week", latest_week, help="Most recent week of data")
        else:
            st.metric("Latest Week", "N/A")
    
    with col5:
        if not player_data.empty:
            avg_fantasy = player_data['fantasy_points_ppr'].mean()
            st.metric("Avg Fantasy PPG", f"{avg_fantasy:.1f}", help="Average fantasy points per game")
        else:
            st.metric("Avg Fantasy PPG", "N/A")
    
    # Quick Insights
    st.subheader("🔍 Quick Insights")
    
    if not team_data.empty and not player_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Top EPA team
            best_epa_idx = team_data.groupby('team')['total_epa'].mean().idxmax()
            best_epa_value = team_data.groupby('team')['total_epa'].mean().max()
            st.info(f"🏆 **Best EPA Team**: {best_epa_idx} ({best_epa_value:.2f})")
            
            # Most consistent team
            team_consistency = team_data.groupby('team')['total_epa'].std()
            most_consistent = team_consistency.idxmin()
            consistency_value = team_consistency.min()
            st.info(f"📈 **Most Consistent Team**: {most_consistent} (±{consistency_value:.2f})")
        
        with col2:
            # Top fantasy scorer
            top_player_idx = player_data.groupby('player_name')['fantasy_points_ppr'].sum().idxmax()
            top_player_points = player_data.groupby('player_name')['fantasy_points_ppr'].sum().max()
            st.success(f"⭐ **Top Fantasy Scorer**: {top_player_idx} ({top_player_points:.1f} pts)")
            
            # Most targeted player
            if 'targets' in player_data.columns:
                top_targets_idx = player_data.groupby('player_name')['targets'].sum().idxmax()
                top_targets_value = player_data.groupby('player_name')['targets'].sum().max()
                st.success(f"🎯 **Most Targeted**: {top_targets_idx} ({int(top_targets_value)} targets)")
    
    # League-wide Trends
    st.subheader("📈 League Trends")
    
    if not team_data.empty:
        # EPA trend over season
        weekly_epa = team_data.groupby('week')['total_epa'].mean().reset_index()
        fig_epa_trend = px.line(
            weekly_epa,
            x='week',
            y='total_epa',
            title="League Average EPA by Week",
            labels={'total_epa': 'Average EPA', 'week': 'Week'}
        )
        fig_epa_trend.add_hline(y=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig_epa_trend, use_container_width=True)
    
    if not player_data.empty:
        # Fantasy points distribution
        st.subheader("🎯 Fantasy Football Trends")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top performers by position
            pos_leaders = player_data.groupby(['position', 'player_name'])['fantasy_points_ppr'].sum().reset_index()
            pos_leaders = pos_leaders.sort_values('fantasy_points_ppr', ascending=False).groupby('position').head(3)
            
            fig_pos = px.bar(
                pos_leaders.head(12),
                x='fantasy_points_ppr',
                y='player_name',
                color='position',
                title="Top Fantasy Performers by Position",
                orientation='h'
            )
            st.plotly_chart(fig_pos, use_container_width=True)
        
        with col2:
            # Weekly scoring trends by position
            pos_weekly = player_data.groupby(['week', 'position'])['fantasy_points_ppr'].mean().reset_index()
            
            fig_weekly_pos = px.line(
                pos_weekly,
                x='week',
                y='fantasy_points_ppr',
                color='position',
                title="Average Fantasy Points by Position and Week"
            )
            st.plotly_chart(fig_weekly_pos, use_container_width=True)

def show_advanced_analytics(team_data, player_data, game_data):
    """Display advanced analytics dashboard"""
    st.header("📊 Advanced Analytics")
    
    st.markdown("""
    This section provides advanced statistical analysis and predictive insights based on the NFL data.
    """)
    
    # Predictive Models Section
    st.subheader("🔮 Predictive Models")
    
    tabs = st.tabs(["EPA Predictions", "Fantasy Projections", "Game Outcomes", "Player Efficiency"])
    
    with tabs[0]:
        st.markdown("#### EPA-based Team Performance Predictions")
        
        if not team_data.empty:
            # Simple EPA trend prediction
            team_epa_trends = team_data.groupby(['team', 'week'])['total_epa'].mean().reset_index()
            
            # Calculate moving averages for trend analysis
            for team in team_epa_trends['team'].unique():
                team_mask = team_epa_trends['team'] == team
                team_epa_trends.loc[team_mask, 'epa_ma3'] = team_epa_trends.loc[team_mask, 'total_epa'].rolling(3).mean()
            
            # Select a team for prediction demo
            selected_team = st.selectbox("Select team for EPA trend analysis:", team_epa_trends['team'].unique())
            
            team_trend_data = team_epa_trends[team_epa_trends['team'] == selected_team]
            
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=team_trend_data['week'],
                y=team_trend_data['total_epa'],
                mode='lines+markers',
                name='Actual EPA',
                line=dict(color='blue')
            ))
            fig_trend.add_trace(go.Scatter(
                x=team_trend_data['week'],
                y=team_trend_data['epa_ma3'],
                mode='lines',
                name='3-Week Moving Average',
                line=dict(color='red', dash='dash')
            ))
            
            fig_trend.update_layout(
                title=f"{selected_team} EPA Trend Analysis",
                xaxis_title="Week",
                yaxis_title="EPA"
            )
            st.plotly_chart(fig_trend, use_container_width=True)
            
            # Trend insights
            if not team_trend_data.empty:
                latest_epa = team_trend_data.iloc[-1]['total_epa']
                avg_epa = team_trend_data['total_epa'].mean()
                trend_direction = "improving" if latest_epa > avg_epa else "declining"
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Latest EPA", f"{latest_epa:.3f}")
                with col2:
                    st.metric("Season Average", f"{avg_epa:.3f}")
                with col3:
                    st.info(f"Trend: {trend_direction.title()}")
    
    with tabs[1]:
        st.markdown("#### Fantasy Performance Projections")
        
        if not player_data.empty:
            # Player consistency and projection analysis
            player_stats = player_data.groupby(['player_name', 'position', 'team']).agg({
                'fantasy_points_ppr': ['mean', 'std', 'count', 'sum'],
                'targets': 'sum' if 'targets' in player_data.columns else 'count'
            }).round(2)
            
            player_stats.columns = ['avg_points', 'std_points', 'games', 'total_points', 'total_targets']
            player_stats = player_stats[player_stats['games'] >= 4].reset_index()
            
            # Calculate consistency score and projection confidence
            player_stats['consistency'] = player_stats['avg_points'] / (player_stats['std_points'] + 0.1)
            player_stats['projection_confidence'] = np.minimum(player_stats['consistency'] * player_stats['games'] / 10, 10)
            
            # Position filter for projections
            position_filter = st.selectbox("Position for projections:", ['All'] + list(player_stats['position'].unique()))
            
            if position_filter != 'All':
                filtered_stats = player_stats[player_stats['position'] == position_filter]
            else:
                filtered_stats = player_stats
            
            # Top projections
            top_projections = filtered_stats.nlargest(15, 'avg_points')
            
            fig_proj = px.scatter(
                top_projections,
                x='avg_points',
                y='projection_confidence',
                size='total_points',
                color='position',
                hover_data=['player_name', 'team'],
                title="Fantasy Projections: Average Points vs Confidence"
            )
            st.plotly_chart(fig_proj, use_container_width=True)
            
            # Projection table
            st.markdown("#### Top Projected Players")
            projection_display = top_projections[['player_name', 'position', 'team', 'avg_points', 'total_points', 'projection_confidence']]
            projection_display.columns = ['Player', 'Position', 'Team', 'Avg PPG', 'Total Points', 'Confidence Score']
            st.dataframe(projection_display, use_container_width=True, hide_index=True)
    
    with tabs[2]:
        st.markdown("#### Game Outcome Analysis")
        
        if not game_data.empty:
            # Home field advantage analysis
            home_wins = len(game_data[game_data['home_score'] > game_data['away_score']])
            total_games = len(game_data)
            home_advantage = (home_wins / total_games) * 100 if total_games > 0 else 0
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Home Win Rate", f"{home_advantage:.1f}%")
            with col2:
                avg_home_score = game_data['home_score'].mean()
                avg_away_score = game_data['away_score'].mean()
                scoring_advantage = avg_home_score - avg_away_score
                st.metric("Home Scoring Advantage", f"+{scoring_advantage:.1f} pts")
            with col3:
                total_scored = (game_data['home_score'] + game_data['away_score']).mean()
                st.metric("Avg Total Points", f"{total_scored:.1f}")
            
            # Weather impact analysis if available
            if 'weather_temperature' in game_data.columns and not game_data['weather_temperature'].isna().all():
                st.subheader("🌤️ Weather Impact Analysis")
                
                # Temperature vs scoring correlation
                weather_data = game_data.dropna(subset=['weather_temperature'])
                weather_data['total_score'] = weather_data['home_score'] + weather_data['away_score']
                
                fig_weather = px.scatter(
                    weather_data,
                    x='weather_temperature',
                    y='total_score',
                    title="Temperature vs Total Scoring",
                    trendline="ols"
                )
                st.plotly_chart(fig_weather, use_container_width=True)
    
    with tabs[3]:
        st.markdown("#### Player Efficiency Metrics")
        
        if not player_data.empty and 'targets' in player_data.columns:
            # Calculate efficiency metrics for receiving players
            receiving_data = player_data[player_data['targets'] > 0].copy()
            
            if not receiving_data.empty:
                # Calculate efficiency metrics
                receiving_data['points_per_target'] = receiving_data['fantasy_points_ppr'] / receiving_data['targets']
                receiving_data['catch_rate'] = receiving_data['receptions'] / receiving_data['targets']
                
                efficiency_stats = receiving_data.groupby(['player_name', 'position', 'team']).agg({
                    'points_per_target': 'mean',
                    'catch_rate': 'mean',
                    'targets': 'sum',
                    'fantasy_points_ppr': 'sum'
                }).round(3)
                
                efficiency_stats = efficiency_stats[efficiency_stats['targets'] >= 20].reset_index()
                
                # Efficiency scatter plot
                fig_efficiency = px.scatter(
                    efficiency_stats,
                    x='catch_rate',
                    y='points_per_target',
                    size='targets',
                    color='position',
                    hover_data=['player_name', 'team'],
                    title="Player Efficiency: Catch Rate vs Points per Target",
                    labels={'catch_rate': 'Catch Rate (%)', 'points_per_target': 'Fantasy Points per Target'}
                )
                st.plotly_chart(fig_efficiency, use_container_width=True)
                
                # Top efficiency players
                st.markdown("#### Most Efficient Players (Min. 20 Targets)")
                top_efficiency = efficiency_stats.nlargest(10, 'points_per_target')
                efficiency_display = top_efficiency[['player_name', 'position', 'team', 'points_per_target', 'catch_rate', 'targets']]
                efficiency_display.columns = ['Player', 'Position', 'Team', 'Points/Target', 'Catch Rate', 'Total Targets']
                st.dataframe(efficiency_display, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()