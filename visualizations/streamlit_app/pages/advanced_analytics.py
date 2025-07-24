"""
Advanced Analytics Dashboard Page - New Staging Models Integration

Shows analytics from new staging models: injuries, depth_charts, snap_counts, 
qbr, ngs_data, and roster data for comprehensive player and team analysis.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from typing import Optional

from ..services.staging_service import get_staging_service


def show_advanced_analytics():
    """Main advanced analytics page."""
    st.title("🔬 Advanced Analytics")
    st.markdown("*Deep dive analytics using all staging models*")
    
    # Get service instance
    service = get_staging_service()
    
    # Sidebar filters
    st.sidebar.header("Filters")
    current_season = datetime.now().year if datetime.now().month >= 9 else datetime.now().year - 1
    
    season = st.sidebar.selectbox(
        "Season",
        options=list(range(2023, current_season + 1)),
        index=len(list(range(2023, current_season + 1))) - 1
    )
    
    week = st.sidebar.selectbox(
        "Week", 
        options=["All"] + list(range(1, 19)),
        index=0
    )
    week_filter = None if week == "All" else week
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🏥 Injury Analytics", 
        "📊 Snap Count Analysis", 
        "🎯 QB Performance", 
        "⚡ Next Gen Stats",
        "👥 Roster Analysis"
    ])
    
    with tab1:
        show_injury_analytics(service, season, week_filter)
    
    with tab2:
        show_snap_count_analysis(service, season, week_filter)
    
    with tab3:
        show_qb_performance(service, season, week_filter)
    
    with tab4:
        show_next_gen_stats(service, season, week_filter)
    
    with tab5:
        show_roster_analysis(service, season, week_filter)


def show_injury_analytics(service, season: int, week: Optional[int]):
    """Show injury report analytics."""
    st.subheader("🏥 Injury Report Analytics")
    
    try:
        # Get injury data
        injuries_df = service.get_injury_reports(season=season, week=week, limit=2000)
        
        if injuries_df.empty:
            st.warning("No injury data available for selected filters.")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Total Injury Reports", len(injuries_df))
            
            # Injury status distribution
            if 'report_status' in injuries_df.columns:
                status_counts = injuries_df['report_status'].value_counts()
                fig = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title="Injury Status Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Teams with most injuries
            if 'team' in injuries_df.columns:
                team_injuries = injuries_df['team'].value_counts().head(10)
                fig = px.bar(
                    x=team_injuries.values,
                    y=team_injuries.index,
                    orientation='h',
                    title="Teams with Most Injury Reports"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Position injury analysis
        if 'position' in injuries_df.columns:
            st.subheader("Injuries by Position")
            position_injuries = injuries_df['position'].value_counts()
            fig = px.bar(
                x=position_injuries.index,
                y=position_injuries.values,
                title="Injury Reports by Position"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Raw data preview
        with st.expander("Raw Injury Data"):
            st.dataframe(injuries_df.head(100))
            
    except Exception as e:
        st.error(f"Error loading injury data: {e}")


def show_snap_count_analysis(service, season: int, week: Optional[int]):
    """Show snap count analytics."""
    st.subheader("📊 Snap Count Analysis")
    
    try:
        snap_counts_df = service.get_snap_counts(season=season, week=week, limit=2000)
        
        if snap_counts_df.empty:
            st.warning("No snap count data available for selected filters.")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Players Tracked", len(snap_counts_df))
            
            # Average offensive snap percentage by position
            if all(col in snap_counts_df.columns for col in ['position', 'offense_pct']):
                avg_snaps = snap_counts_df.groupby('position')['offense_pct'].mean().sort_values(ascending=False)
                fig = px.bar(
                    x=avg_snaps.index,
                    y=avg_snaps.values,
                    title="Average Offensive Snap % by Position"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Top snap count leaders
            if 'offense_snaps' in snap_counts_df.columns:
                top_snaps = snap_counts_df.nlargest(10, 'offense_snaps')[['player_name', 'position', 'team', 'offense_snaps']]
                st.subheader("Top Offensive Snap Leaders")
                st.dataframe(top_snaps)
        
        # Snap count trends (if weekly data available)
        if 'week' in snap_counts_df.columns and week is None:
            st.subheader("Snap Count Trends Over Season")
            weekly_avg = snap_counts_df.groupby('week')['offense_pct'].mean()
            fig = px.line(
                x=weekly_avg.index,
                y=weekly_avg.values,
                title="Average Offensive Snap % by Week"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("Raw Snap Count Data"):
            st.dataframe(snap_counts_df.head(100))
            
    except Exception as e:
        st.error(f"Error loading snap count data: {e}")


def show_qb_performance(service, season: int, week: Optional[int]):
    """Show QB performance analytics."""
    st.subheader("🎯 Quarterback Performance (QBR)")
    
    try:
        qbr_df = service.get_qbr_data(season=season, week=week, limit=1000)
        
        if qbr_df.empty:
            st.warning("No QBR data available for selected filters.")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("QB Performances", len(qbr_df))
            
            # Top QBR performers
            if 'qbr_total' in qbr_df.columns:
                top_qbr = qbr_df.nlargest(10, 'qbr_total')[['player_name', 'team', 'qbr_total']]
                st.subheader("Top QBR Performances")
                st.dataframe(top_qbr)
        
        with col2:
            # QBR distribution
            if 'qbr_total' in qbr_df.columns:
                fig = px.histogram(
                    qbr_df,
                    x='qbr_total',
                    title="QBR Distribution",
                    nbins=20
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Passing vs Rushing QBR
        if all(col in qbr_df.columns for col in ['pass_qbr', 'rush_qbr']):
            st.subheader("Passing vs Rushing QBR")
            fig = px.scatter(
                qbr_df,
                x='pass_qbr',
                y='rush_qbr',
                hover_data=['player_name', 'team'],
                title="Passing QBR vs Rushing QBR"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("Raw QBR Data"):
            st.dataframe(qbr_df.head(100))
            
    except Exception as e:
        st.error(f"Error loading QBR data: {e}")


def show_next_gen_stats(service, season: int, week: Optional[int]):
    """Show Next Gen Stats analytics."""
    st.subheader("⚡ Next Gen Stats Analytics")
    
    try:
        ngs_df = service.get_ngs_data(season=season, week=week, limit=1000)
        
        if ngs_df.empty:
            st.warning("No Next Gen Stats data available for selected filters.")
            return
        
        # Position filter
        positions = ngs_df['position'].unique() if 'position' in ngs_df.columns else []
        selected_position = st.selectbox("Select Position", ["All"] + list(positions))
        
        if selected_position != "All":
            ngs_df = ngs_df[ngs_df['position'] == selected_position]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Player Performances", len(ngs_df))
            
            # Air yards analysis (for passing)
            if 'avg_completed_air_yards' in ngs_df.columns:
                air_yards = ngs_df.dropna(subset=['avg_completed_air_yards'])
                if not air_yards.empty:
                    fig = px.histogram(
                        air_yards,
                        x='avg_completed_air_yards',
                        title="Average Completed Air Yards Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Separation analysis (for receiving)
            if 'avg_separation' in ngs_df.columns:
                separation = ngs_df.dropna(subset=['avg_separation'])
                if not separation.empty:
                    top_separation = separation.nlargest(10, 'avg_separation')[['player_name', 'team', 'avg_separation']]
                    st.subheader("Top Average Separation")
                    st.dataframe(top_separation)
        
        # Time to throw vs completion percentage
        if all(col in ngs_df.columns for col in ['avg_time_to_throw', 'completions', 'attempts']):
            passing_data = ngs_df.dropna(subset=['avg_time_to_throw', 'completions', 'attempts'])
            if not passing_data.empty:
                passing_data['completion_pct'] = passing_data['completions'] / passing_data['attempts'] * 100
                
                fig = px.scatter(
                    passing_data,
                    x='avg_time_to_throw',
                    y='completion_pct',
                    hover_data=['player_name', 'team'],
                    title="Time to Throw vs Completion %"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("Raw Next Gen Stats Data"):
            st.dataframe(ngs_df.head(100))
            
    except Exception as e:
        st.error(f"Error loading Next Gen Stats data: {e}")


def show_roster_analysis(service, season: int, week: Optional[int]):
    """Show roster analytics."""
    st.subheader("👥 Roster Analysis")
    
    try:
        roster_df = service.get_weekly_rosters(season=season, week=week, limit=3000)
        
        if roster_df.empty:
            st.warning("No roster data available for selected filters.")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Total Roster Entries", len(roster_df))
            
            # Players by position
            if 'position' in roster_df.columns:
                position_counts = roster_df['position'].value_counts()
                fig = px.pie(
                    values=position_counts.values,
                    names=position_counts.index,
                    title="Roster Distribution by Position"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Experience analysis
            if 'years_exp' in roster_df.columns:
                exp_data = roster_df.dropna(subset=['years_exp'])
                if not exp_data.empty:
                    fig = px.histogram(
                        exp_data,
                        x='years_exp',
                        title="Experience Distribution (Years)",
                        nbins=15
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        # College representation
        if 'college' in roster_df.columns:
            st.subheader("Top Colleges Represented")
            college_counts = roster_df['college'].value_counts().head(15)
            fig = px.bar(
                x=college_counts.values,
                y=college_counts.index,
                orientation='h',
                title="Players by College"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("Raw Roster Data"):
            st.dataframe(roster_df.head(100))
            
    except Exception as e:
        st.error(f"Error loading roster data: {e}")


if __name__ == "__main__":
    show_advanced_analytics()