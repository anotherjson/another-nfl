"""
Enhanced NFL Analytics Streamlit Dashboard

Interactive analytics dashboard that leverages dbt staging models for improved data consistency
and integration with the extraction pipeline and Dagster orchestration.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import numpy as np
from pathlib import Path

# Import enhanced dbt connection utilities
from utils.dbt_connection import get_dbt_connection, query_staging_table, get_staging_table_summary
from pages.staging_explorer import show_staging_explorer

# Configure page
st.set_page_config(
    page_title="NFL Analytics Dashboard - Enhanced",
    page_icon="🏈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enhanced data loading functions using dbt staging tables
@st.cache_data(ttl=300)
def load_team_data_from_staging():
    """Load team data from dbt staging table"""
    try:
        df = query_staging_table("stg_team_desc_enhanced", limit=50)
        if df.empty:
            st.warning("No team data available from staging table. Using fallback.")
            return load_team_data_fallback()
        return df
    except Exception as e:
        st.error(f"Error loading team data from staging: {e}")
        return load_team_data_fallback()

@st.cache_data(ttl=300)
def load_weekly_data_from_staging():
    """Load weekly data from dbt staging table"""
    try:
        df = query_staging_table("stg_weekly_enhanced", limit=2000)
        if df.empty:
            st.warning("No weekly data available from staging table. Using fallback.")
            return load_weekly_data_fallback()
        return df
    except Exception as e:
        st.error(f"Error loading weekly data from staging: {e}")
        return load_weekly_data_fallback()

@st.cache_data(ttl=300)
def load_schedule_data_from_staging():
    """Load schedule data from dbt staging table"""
    try:
        df = query_staging_table("stg_schedules_enhanced", limit=1000)
        if df.empty:
            st.warning("No schedule data available from staging table. Using fallback.")
            return load_schedule_data_fallback()
        return df
    except Exception as e:
        st.error(f"Error loading schedule data from staging: {e}")
        return load_schedule_data_fallback()

# Fallback functions (original data loading)
@st.cache_data
def load_team_data_fallback():
    """Fallback: Load team data from parquet files"""
    try:
        team_data_path = Path("data/team_desc")
        parquet_files = list(team_data_path.glob("**/data.parquet"))
        if parquet_files:
            return pd.read_parquet(parquet_files[0])
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading team data fallback: {e}")
        return pd.DataFrame()

@st.cache_data 
def load_weekly_data_fallback():
    """Fallback: Load weekly data from parquet files"""
    try:
        weekly_data_path = Path("data/weekly")
        parquet_files = list(weekly_data_path.glob("**/data.parquet"))
        if parquet_files:
            df = pd.read_parquet(parquet_files[-1])
            return df.head(1000)  # Limit for performance
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading weekly data fallback: {e}")
        return pd.DataFrame()

@st.cache_data
def load_schedule_data_fallback():
    """Fallback: Load schedule data from parquet files"""
    try:
        schedule_data_path = Path("data/schedules")
        parquet_files = list(schedule_data_path.glob("**/data.parquet"))
        if parquet_files:
            return pd.read_parquet(parquet_files[-1])
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading schedule data fallback: {e}")
        return pd.DataFrame()

def main():
    """Main dashboard function with enhanced dbt integration"""
    
    st.title("🏈 NFL Analytics Dashboard - Enhanced")
    st.markdown("Interactive NFL data visualization powered by dbt staging models and Streamlit")
    
    # Display data pipeline status
    show_pipeline_status()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Choose a page", [
        "Overview", 
        "Team Analysis", 
        "Player Stats",
        "Schedule Analysis",
        "dbt Staging Explorer"  # New page
    ])
    
    if page == "Overview":
        show_overview()
    elif page == "Team Analysis":
        show_team_analysis()
    elif page == "Player Stats": 
        show_player_stats()
    elif page == "Schedule Analysis":
        show_schedule_analysis()
    elif page == "dbt Staging Explorer":
        show_staging_explorer()

def show_pipeline_status():
    """Show data pipeline and dbt model status"""
    with st.expander("📊 Data Pipeline Status", expanded=False):
        try:
            summary = get_staging_table_summary()
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Available Staging Tables", len(summary["available_tables"]))
            
            with col2:
                total_rows = sum(
                    info.get("row_count", 0) 
                    for info in summary["table_info"].values() 
                    if info.get("table_exists", False)
                )
                st.metric("Total Staging Rows", f"{total_rows:,}")
            
            with col3:
                st.metric("Last Updated", summary["last_updated"][:16])
            
            # Show table status
            if summary["available_tables"]:
                st.write("**Available Tables:**")
                for table in summary["available_tables"]:
                    info = summary["table_info"].get(table, {})
                    if info.get("table_exists", False):
                        st.write(f"✅ {table}: {info.get('row_count', 0):,} rows")
                    else:
                        st.write(f"❌ {table}: Not available")
        except Exception as e:
            st.error(f"Could not load pipeline status: {e}")

def show_overview():
    """Enhanced overview page with dbt staging data"""
    st.header("NFL Data Overview - Enhanced")
    
    col1, col2, col3 = st.columns(3)
    
    # Team data overview
    team_df = load_team_data_from_staging()
    if not team_df.empty:
        with col1:
            st.metric("NFL Teams", len(team_df))
            if "conference_full_name" in team_df.columns:
                st.write("**By Conference:**")
                conf_counts = team_df["team_conference"].value_counts()
                for conf, count in conf_counts.items():
                    st.write(f"• {conf}: {count} teams")
            
    # Weekly data overview  
    weekly_df = load_weekly_data_from_staging()
    if not weekly_df.empty:
        with col2:
            st.metric("Player Records", len(weekly_df))
            if "position_group" in weekly_df.columns:
                st.write("**By Position:**")
                pos_counts = weekly_df["position_group"].value_counts().head(3)
                for pos, count in pos_counts.items():
                    st.write(f"• {pos}: {count}")
            
    # Schedule data overview
    schedule_df = load_schedule_data_from_staging()
    if not schedule_df.empty:
        with col3:
            st.metric("Games", len(schedule_df))
            if "season" in schedule_df.columns:
                seasons = sorted(schedule_df["season"].unique())
                st.write(f"**Seasons:** {min(seasons)}-{max(seasons)}")
    
    # Enhanced data quality checks
    st.header("Data Quality Status - Enhanced")
    
    status_data = []
    
    # Team data status
    team_status = "✅ Available (dbt staging)" if not team_df.empty and "dbt_model" in team_df.columns else "⚠️ Available (fallback)" if not team_df.empty else "❌ Missing"
    status_data.append(["Teams", team_status, len(team_df)])
    
    # Weekly data status  
    weekly_status = "✅ Available (dbt staging)" if not weekly_df.empty and "dbt_model" in weekly_df.columns else "⚠️ Available (fallback)" if not weekly_df.empty else "❌ Missing"
    status_data.append(["Weekly Stats", weekly_status, len(weekly_df)])
    
    # Schedule data status
    schedule_status = "✅ Available (dbt staging)" if not schedule_df.empty and "dbt_model" in schedule_df.columns else "⚠️ Available (fallback)" if not schedule_df.empty else "❌ Missing"
    status_data.append(["Schedules", schedule_status, len(schedule_df)])
    
    status_df = pd.DataFrame(status_data, columns=["Dataset", "Status", "Records"])
    st.table(status_df)
    
    # Data lineage information
    if not team_df.empty and "dbt_loaded_at" in team_df.columns:
        st.info(f"ℹ️ Data processed through dbt staging models. Last dbt run: {team_df['dbt_loaded_at'].iloc[0]}")

def show_team_analysis():
    """Enhanced team analysis using staging data"""
    st.header("Team Analysis - Enhanced")
    
    team_df = load_team_data_from_staging()
    if team_df.empty:
        st.warning("No team data available")
        return
        
    st.subheader("NFL Teams Distribution")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Conference distribution
        if "team_conference" in team_df.columns:
            conf_counts = team_df["team_conference"].value_counts()
            fig = px.pie(
                values=conf_counts.values,
                names=conf_counts.index,
                title="Teams by Conference",
                color_discrete_map={"AFC": "#FF6B6B", "NFC": "#4ECDC4"}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Division distribution with enhanced styling
        if "team_division" in team_df.columns:
            div_counts = team_df["team_division"].value_counts()
            fig = px.bar(
                x=div_counts.values,
                y=div_counts.index,
                orientation='h',
                title="Teams by Division",
                color=div_counts.values,
                color_continuous_scale="viridis"
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    # Enhanced team grid with colors
    st.subheader("Team Details - Enhanced")
    
    # Display columns selection
    display_cols = ["team_abbr", "full_team_name", "conference_full_name", "division_display"]
    if "dbt_loaded_at" in team_df.columns:
        display_cols.append("dbt_loaded_at")
    
    available_cols = [col for col in display_cols if col in team_df.columns]
    st.dataframe(team_df[available_cols], use_container_width=True)

def show_player_stats():
    """Enhanced player statistics using staging data"""
    st.header("Player Statistics - Enhanced")
    
    weekly_df = load_weekly_data_from_staging()
    if weekly_df.empty:
        st.warning("No player data available")
        return
    
    # Enhanced filtering
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if "season" in weekly_df.columns:
            seasons = ["All"] + sorted(weekly_df["season"].unique(), reverse=True)
            selected_season = st.selectbox("Season", seasons)
            if selected_season != "All":
                weekly_df = weekly_df[weekly_df["season"] == selected_season]
    
    with col2:
        if "position_group" in weekly_df.columns:
            positions = ["All"] + sorted(weekly_df["position_group"].unique())
            selected_position = st.selectbox("Position Group", positions)
            if selected_position != "All":
                weekly_df = weekly_df[weekly_df["position_group"] == selected_position]
    
    with col3:
        if "team" in weekly_df.columns:
            teams = ["All"] + sorted(weekly_df["team"].unique())
            selected_team = st.selectbox("Team", teams)
            if selected_team != "All":
                weekly_df = weekly_df[weekly_df["team"] == selected_team]
    
    # Enhanced visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        # Position distribution with enhanced styling
        if "position_group" in weekly_df.columns:
            st.subheader("Players by Position Group")
            position_counts = weekly_df["position_group"].value_counts()
            
            fig = px.bar(
                x=position_counts.values,
                y=position_counts.index,
                orientation='h',
                title="Player Count by Position Group",
                color=position_counts.values,
                color_continuous_scale="Blues"
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Fantasy performance tiers
        if "fantasy_performance_tier" in weekly_df.columns:
            st.subheader("Fantasy Performance Distribution")
            tier_counts = weekly_df["fantasy_performance_tier"].value_counts()
            
            colors = {
                "Elite (20+)": "#FFD700",
                "Great (15-19.9)": "#32CD32", 
                "Good (10-14.9)": "#87CEEB",
                "Okay (5-9.9)": "#DDA0DD",
                "Poor (0.1-4.9)": "#F0E68C",
                "Zero (0)": "#D3D3D3"
            }
            
            fig = px.pie(
                values=tier_counts.values,
                names=tier_counts.index,
                title="Fantasy Performance Tiers",
                color=tier_counts.index,
                color_discrete_map=colors
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Top performers table
    if "fantasy_points_ppr" in weekly_df.columns:
        st.subheader("Top Fantasy Performers")
        top_cols = ["player_name", "position_group", "team", "fantasy_points_ppr"]
        if "total_yards" in weekly_df.columns:
            top_cols.append("total_yards")
        if "total_touchdowns" in weekly_df.columns:
            top_cols.append("total_touchdowns")
        
        available_top_cols = [col for col in top_cols if col in weekly_df.columns]
        top_performers = weekly_df.nlargest(10, "fantasy_points_ppr")[available_top_cols]
        st.dataframe(top_performers, use_container_width=True)

def show_schedule_analysis():
    """Enhanced schedule analysis using staging data"""
    st.header("Schedule Analysis - Enhanced")
    
    schedule_df = load_schedule_data_from_staging()
    if schedule_df.empty:
        st.warning("No schedule data available")
        return
    
    # Season filter
    if "season" in schedule_df.columns:
        seasons = ["All"] + sorted(schedule_df["season"].unique(), reverse=True)
        selected_season = st.selectbox("Season", seasons)
        if selected_season != "All":
            schedule_df = schedule_df[schedule_df["season"] == selected_season]
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Games by week with enhanced styling
        if "week" in schedule_df.columns:
            st.subheader("Games by Week")
            week_counts = schedule_df.groupby("week").size().reset_index(name="games")
            
            fig = px.line(
                week_counts,
                x="week",
                y="games",
                title="Games per Week",
                markers=True
            )
            fig.update_traces(line_color="#FF6B6B", marker_color="#FF6B6B")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Game competitiveness
        if "game_competitiveness" in schedule_df.columns:
            st.subheader("Game Competitiveness")
            comp_counts = schedule_df["game_competitiveness"].value_counts()
            
            fig = px.bar(
                x=comp_counts.index,
                y=comp_counts.values,
                title="Game Competitiveness Distribution",
                color=comp_counts.values,
                color_continuous_scale="RdYlBu_r"
            )
            fig.update_xaxis(tickangle=45)
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    # Scoring analysis
    if "total_points" in schedule_df.columns:
        st.subheader("Scoring Analysis")
        
        col3, col4 = st.columns(2)
        
        with col3:
            avg_points = schedule_df["total_points"].mean()
            max_points = schedule_df["total_points"].max()
            min_points = schedule_df["total_points"].min()
            
            st.metric("Average Total Points", f"{avg_points:.1f}")
            st.metric("Highest Scoring Game", f"{max_points}")
            st.metric("Lowest Scoring Game", f"{min_points}")
        
        with col4:
            fig = px.histogram(
                schedule_df,
                x="total_points",
                title="Distribution of Total Points Scored",
                nbins=20,
                color_discrete_sequence=["#4ECDC4"]
            )
            st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()