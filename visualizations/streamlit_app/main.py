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
    
    # Header
    st.title("🏈 NFL Analytics Dashboard")
    st.markdown("Interactive analytics for NFL data powered by DuckDB and dbt")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a dashboard:",
        ["Team Performance", "Player Analytics", "Fantasy Football", "Betting Intelligence"]
    )
    
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
    if page == "Team Performance":
        show_team_performance(team_data)
    elif page == "Player Analytics":
        show_player_analytics(player_data)
    elif page == "Fantasy Football":
        show_fantasy_dashboard(player_data)
    elif page == "Betting Intelligence":
        show_betting_dashboard(game_data)

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

if __name__ == "__main__":
    main()