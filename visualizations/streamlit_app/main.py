
"""
NFL Analytics Streamlit Dashboard

Interactive analytics dashboard for NFL data visualization.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

# Data loading functions
@st.cache_data
def load_team_data():
    """Load team description data"""
    try:
        team_data_path = Path("data/team_desc")
        parquet_files = list(team_data_path.glob("**/data.parquet"))
        if parquet_files:
            return pd.read_parquet(parquet_files[0])
        else:
            st.error("No team data found")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading team data: {e}")
        return pd.DataFrame()

@st.cache_data 
def load_weekly_data():
    """Load weekly player data"""
    try:
        weekly_data_path = Path("data/weekly")
        parquet_files = list(weekly_data_path.glob("**/data.parquet"))
        if parquet_files:
            # Load most recent file
            df = pd.read_parquet(parquet_files[-1])
            return df.head(1000)  # Limit for performance
        else:
            st.error("No weekly data found")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading weekly data: {e}")
        return pd.DataFrame()

@st.cache_data
def load_schedule_data():
    """Load schedule data"""
    try:
        schedule_data_path = Path("data/schedules")  
        parquet_files = list(schedule_data_path.glob("**/data.parquet"))
        if parquet_files:
            return pd.read_parquet(parquet_files[-1])
        else:
            st.error("No schedule data found")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading schedule data: {e}")
        return pd.DataFrame()

def main():
    """Main dashboard function"""
    
    st.title("🏈 NFL Analytics Dashboard")
    st.markdown("Interactive NFL data visualization powered by Streamlit")
    
    # Sidebar
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
    """Show overview page"""
    st.header("NFL Data Overview")
    
    col1, col2, col3 = st.columns(3)
    
    # Team data overview
    team_df = load_team_data()
    if not team_df.empty:
        with col1:
            st.metric("NFL Teams", len(team_df))
            
    # Weekly data overview  
    weekly_df = load_weekly_data()
    if not weekly_df.empty:
        with col2:
            st.metric("Player Records", len(weekly_df))
            
    # Schedule data overview
    schedule_df = load_schedule_data()
    if not schedule_df.empty:
        with col3:
            st.metric("Games", len(schedule_df))
    
    # Data quality checks
    st.header("Data Quality Status")
    
    status_data = {
        "Dataset": ["Teams", "Weekly Stats", "Schedules"],
        "Status": ["✅ Available" if not team_df.empty else "❌ Missing",
                  "✅ Available" if not weekly_df.empty else "❌ Missing", 
                  "✅ Available" if not schedule_df.empty else "❌ Missing"],
        "Records": [len(team_df), len(weekly_df), len(schedule_df)]
    }
    
    st.table(pd.DataFrame(status_data))

def show_team_analysis():
    """Show team analysis page"""
    st.header("Team Analysis")
    
    team_df = load_team_data()
    if team_df.empty:
        st.warning("No team data available")
        return
        
    st.subheader("NFL Teams Distribution")
    
    # Team count by division (if available)
    if 'team_division' in team_df.columns:
        fig = px.bar(
            team_df.groupby('team_division').size().reset_index(name='count'),
            x='team_division',
            y='count',
            title="Teams by Division"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Team details table
    st.subheader("Team Details")
    st.dataframe(team_df, use_container_width=True)

def show_player_stats():
    """Show player statistics page"""
    st.header("Player Statistics")
    
    weekly_df = load_weekly_data()
    if weekly_df.empty:
        st.warning("No player data available")
        return
        
    # Position distribution (if available)
    if 'position' in weekly_df.columns:
        st.subheader("Player Positions")
        position_counts = weekly_df['position'].value_counts()
        
        fig = px.pie(
            values=position_counts.values,
            names=position_counts.index,
            title="Players by Position"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Player stats sample
    st.subheader("Player Data Sample")  
    st.dataframe(weekly_df.head(20), use_container_width=True)

def show_schedule_analysis():
    """Show schedule analysis page"""
    st.header("Schedule Analysis")
    
    schedule_df = load_schedule_data()
    if schedule_df.empty:
        st.warning("No schedule data available")
        return
        
    # Games by week (if available)
    if 'week' in schedule_df.columns:
        st.subheader("Games by Week")
        week_counts = schedule_df.groupby('week').size()
        
        fig = go.Figure(data=go.Bar(
            x=week_counts.index,
            y=week_counts.values,
            name='Games per Week'
        ))
        
        fig.update_layout(
            title="NFL Games by Week",
            xaxis_title="Week",
            yaxis_title="Number of Games"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Schedule sample
    st.subheader("Schedule Data Sample")
    st.dataframe(schedule_df.head(20), use_container_width=True)

if __name__ == "__main__":
    main()
