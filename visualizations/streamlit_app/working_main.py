"""
NFL Analytics Streamlit Dashboard - Working Version

Simplified dashboard that works directly with parquet files and DuckDB views.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb
from pathlib import Path
from datetime import datetime

# Configure page
st.set_page_config(
    page_title="NFL Analytics Dashboard",
    page_icon="🏈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize DuckDB connection
@st.cache_resource
def get_duckdb_conn():
    """Get DuckDB connection and create views"""
    conn = duckdb.connect("data/streamlit_nfl.duckdb")
    
    # Create staging views from parquet files
    try:
        # Team data view
        conn.execute("""
            CREATE VIEW IF NOT EXISTS teams AS
            SELECT 
                team_abbr,
                team_name,
                team_conf as conference,
                team_division as division,
                team_color as primary_color,
                team_color2 as secondary_color
            FROM '../../data/team_desc/etl_date=*/data.parquet'
        """)
        
        # Weekly stats view (limited for performance)
        conn.execute("""
            CREATE VIEW IF NOT EXISTS weekly_stats AS
            SELECT 
                player_id,
                player_display_name as player_name,
                position,
                recent_team as team,
                season,
                week,
                CASE 
                    WHEN position IN ('QB') THEN 'Quarterback'
                    WHEN position IN ('RB', 'FB') THEN 'Running Back'
                    WHEN position IN ('WR') THEN 'Wide Receiver'
                    WHEN position IN ('TE') THEN 'Tight End'
                    WHEN position IN ('K') THEN 'Kicker'
                    WHEN position IN ('DEF') THEN 'Defense'
                    ELSE 'Other'
                END as position_group,
                COALESCE(fantasy_points_ppr, 0) as fantasy_points,
                COALESCE(passing_yards, 0) + COALESCE(rushing_yards, 0) + COALESCE(receiving_yards, 0) as total_yards,
                COALESCE(passing_tds, 0) + COALESCE(rushing_tds, 0) + COALESCE(receiving_tds, 0) as total_tds
            FROM '../../data/weekly/*/etl_date=*/data.parquet'
            LIMIT 10000
        """)
        
        # Schedule data view
        conn.execute("""
            CREATE VIEW IF NOT EXISTS schedules AS
            SELECT 
                game_id,
                season,
                week,
                home_team,
                away_team,
                home_score,
                away_score,
                CASE 
                    WHEN home_score > away_score THEN home_team
                    WHEN away_score > home_score THEN away_team
                    ELSE 'TIE'
                END as winner,
                home_score + away_score as total_points,
                ABS(home_score - away_score) as point_diff
            FROM '../../data/schedules/*/etl_date=*/data.parquet'
        """)
        
    except Exception as e:
        st.error(f"Error creating views: {e}")
    
    return conn

# Data loading functions
@st.cache_data(ttl=300)
def load_teams():
    """Load team data"""
    try:
        conn = get_duckdb_conn()
        return conn.execute("SELECT * FROM teams").df()
    except Exception as e:
        st.error(f"Error loading teams: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_weekly_stats():
    """Load weekly stats"""
    try:
        conn = get_duckdb_conn()
        return conn.execute("SELECT * FROM weekly_stats").df()
    except Exception as e:
        st.error(f"Error loading weekly stats: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_schedules():
    """Load schedule data"""
    try:
        conn = get_duckdb_conn()
        return conn.execute("SELECT * FROM schedules").df()
    except Exception as e:
        st.error(f"Error loading schedules: {e}")
        return pd.DataFrame()

def main():
    """Main dashboard"""
    st.title("🏈 NFL Analytics Dashboard")
    st.markdown("Interactive NFL data visualization with DuckDB staging")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Choose a page", [
        "Overview", 
        "Team Analysis", 
        "Player Stats",
        "Schedule Analysis"
    ])
    
    if page == "Overview":
        show_overview()
    elif page == "Team Analysis":
        show_team_analysis()
    elif page == "Player Stats":
        show_player_stats()
    elif page == "Schedule Analysis":
        show_schedule_analysis()

def show_overview():
    """Overview page"""
    st.header("NFL Data Overview")
    
    col1, col2, col3 = st.columns(3)
    
    # Load data
    teams_df = load_teams()
    weekly_df = load_weekly_stats()
    schedules_df = load_schedules()
    
    with col1:
        st.metric("NFL Teams", len(teams_df))
        if not teams_df.empty and 'conference' in teams_df.columns:
            conf_counts = teams_df['conference'].value_counts()
            for conf, count in conf_counts.items():
                st.write(f"• {conf}: {count}")
    
    with col2:
        st.metric("Player Records", len(weekly_df))
        if not weekly_df.empty and 'season' in weekly_df.columns:
            seasons = sorted(weekly_df['season'].unique())
            st.write(f"Seasons: {min(seasons)}-{max(seasons)}")
    
    with col3:
        st.metric("Games", len(schedules_df))
        if not schedules_df.empty and 'season' in schedules_df.columns:
            seasons = sorted(schedules_df['season'].unique())
            st.write(f"Seasons: {min(seasons)}-{max(seasons)}")
    
    # Data status
    st.header("Data Status")
    status_data = {
        "Dataset": ["Teams", "Player Stats", "Schedules"],
        "Status": [
            "✅ Available" if not teams_df.empty else "❌ Missing",
            "✅ Available" if not weekly_df.empty else "❌ Missing",
            "✅ Available" if not schedules_df.empty else "❌ Missing"
        ],
        "Records": [len(teams_df), len(weekly_df), len(schedules_df)]
    }
    st.table(pd.DataFrame(status_data))

def show_team_analysis():
    """Team analysis page"""
    st.header("Team Analysis")
    
    teams_df = load_teams()
    if teams_df.empty:
        st.warning("No team data available")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Conference distribution
        if 'conference' in teams_df.columns:
            conf_counts = teams_df['conference'].value_counts()
            fig = px.pie(
                values=conf_counts.values,
                names=conf_counts.index,
                title="Teams by Conference",
                color_discrete_map={"AFC": "#FF6B6B", "NFC": "#4ECDC4"}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Division distribution
        if 'division' in teams_df.columns:
            div_counts = teams_df['division'].value_counts()
            fig = px.bar(
                x=div_counts.values,
                y=div_counts.index,
                orientation='h',
                title="Teams by Division"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Team table
    st.subheader("Team Details")
    st.dataframe(teams_df, use_container_width=True)

def show_player_stats():
    """Player stats page"""
    st.header("Player Statistics")
    
    weekly_df = load_weekly_stats()
    if weekly_df.empty:
        st.warning("No player data available")
        return
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'season' in weekly_df.columns:
            seasons = ["All"] + sorted(weekly_df['season'].unique(), reverse=True)
            selected_season = st.selectbox("Season", seasons)
            if selected_season != "All":
                weekly_df = weekly_df[weekly_df['season'] == selected_season]
    
    with col2:
        if 'position_group' in weekly_df.columns:
            positions = ["All"] + sorted(weekly_df['position_group'].unique())
            selected_position = st.selectbox("Position Group", positions)
            if selected_position != "All":
                weekly_df = weekly_df[weekly_df['position_group'] == selected_position]
    
    with col3:
        if 'team' in weekly_df.columns:
            teams = ["All"] + sorted(weekly_df['team'].unique())
            selected_team = st.selectbox("Team", teams)
            if selected_team != "All":
                weekly_df = weekly_df[weekly_df['team'] == selected_team]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Position distribution
        if 'position_group' in weekly_df.columns:
            pos_counts = weekly_df['position_group'].value_counts()
            fig = px.bar(
                x=pos_counts.values,
                y=pos_counts.index,
                orientation='h',
                title="Players by Position"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Fantasy points distribution
        if 'fantasy_points' in weekly_df.columns:
            fig = px.histogram(
                weekly_df,
                x='fantasy_points',
                title="Fantasy Points Distribution",
                nbins=30
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Top performers
    if 'fantasy_points' in weekly_df.columns:
        st.subheader("Top Fantasy Performers")
        top_cols = ['player_name', 'position_group', 'team', 'fantasy_points']
        if 'total_yards' in weekly_df.columns:
            top_cols.append('total_yards')
        if 'total_tds' in weekly_df.columns:
            top_cols.append('total_tds')
        
        available_cols = [col for col in top_cols if col in weekly_df.columns]
        top_performers = weekly_df.nlargest(15, 'fantasy_points')[available_cols]
        st.dataframe(top_performers, use_container_width=True)

def show_schedule_analysis():
    """Schedule analysis page"""
    st.header("Schedule Analysis")
    
    schedules_df = load_schedules()
    if schedules_df.empty:
        st.warning("No schedule data available")
        return
    
    # Season filter
    if 'season' in schedules_df.columns:
        seasons = ["All"] + sorted(schedules_df['season'].unique(), reverse=True)
        selected_season = st.selectbox("Season", seasons)
        if selected_season != "All":
            schedules_df = schedules_df[schedules_df['season'] == selected_season]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Games by week
        if 'week' in schedules_df.columns:
            week_counts = schedules_df['week'].value_counts().sort_index()
            fig = px.line(
                x=week_counts.index,
                y=week_counts.values,
                title="Games per Week",
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Scoring distribution
        if 'total_points' in schedules_df.columns:
            fig = px.histogram(
                schedules_df,
                x='total_points',
                title="Total Points Distribution",
                nbins=25
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Summary stats
    if 'total_points' in schedules_df.columns:
        st.subheader("Scoring Summary")
        col3, col4, col5 = st.columns(3)
        
        with col3:
            st.metric("Average Total Points", f"{schedules_df['total_points'].mean():.1f}")
        with col4:
            st.metric("Highest Scoring Game", schedules_df['total_points'].max())
        with col5:
            st.metric("Lowest Scoring Game", schedules_df['total_points'].min())
    
    # Recent games sample
    st.subheader("Recent Games")
    display_cols = ['season', 'week', 'home_team', 'away_team', 'home_score', 'away_score', 'winner']
    available_cols = [col for col in display_cols if col in schedules_df.columns]
    st.dataframe(schedules_df[available_cols].head(20), use_container_width=True)

if __name__ == "__main__":
    main()