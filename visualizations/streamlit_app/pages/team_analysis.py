"""
Team Analysis Dashboard Page

Detailed team performance analysis and comparisons.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import os

def show_team_analysis():
    """Show detailed team analysis dashboard"""
    
    st.title("🏈 Team Performance Analysis")
    
    # Load team data
    conn = duckdb.connect(os.getenv("NFL_DATA_PATH", "/data") + "/nfl_analytics.duckdb", read_only=True)
    
    # Team performance query
    team_query = """
    WITH team_rankings AS (
        SELECT 
            team,
            week,
            season,
            total_epa,
            pass_epa,
            rush_epa,
            wins,
            losses,
            ROW_NUMBER() OVER (PARTITION BY week ORDER BY total_epa DESC) as epa_rank,
            AVG(total_epa) OVER (PARTITION BY team ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_avg_epa
        FROM mart_weekly_team_stats
        WHERE season = 2023
    )
    SELECT * FROM team_rankings
    ORDER BY week, epa_rank
    """
    
    team_data = conn.execute(team_query).df()
    
    if team_data.empty:
        st.warning("No team data available. Make sure dbt models have been run.")
        return
    
    # Sidebar filters
    st.sidebar.subheader("Team Analysis Filters")
    
    # Week range selector
    min_week = int(team_data['week'].min())
    max_week = int(team_data['week'].max())
    week_range = st.sidebar.slider(
        "Week Range", 
        min_week, max_week, 
        (min_week, max_week)
    )
    
    # Team selector
    all_teams = sorted(team_data['team'].unique())
    selected_teams = st.sidebar.multiselect(
        "Select Teams", 
        all_teams, 
        default=all_teams[:8]
    )
    
    # Filter data
    filtered_data = team_data[
        (team_data['week'] >= week_range[0]) & 
        (team_data['week'] <= week_range[1]) &
        (team_data['team'].isin(selected_teams) if selected_teams else True)
    ]
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        best_epa_team = filtered_data.loc[filtered_data['total_epa'].idxmax(), 'team']
        best_epa_value = filtered_data['total_epa'].max()
        st.metric("Best EPA Performance", f"{best_epa_value:.2f}", best_epa_team)
    
    with col2:
        most_consistent = filtered_data.groupby('team')['total_epa'].std().idxmin()
        consistency_std = filtered_data.groupby('team')['total_epa'].std().min()
        st.metric("Most Consistent", f"±{consistency_std:.2f}", most_consistent)
    
    with col3:
        avg_epa = filtered_data['total_epa'].mean()
        st.metric("League Avg EPA", f"{avg_epa:.2f}")
    
    with col4:
        total_games = len(filtered_data)
        st.metric("Games Analyzed", total_games)
    
    # Main visualizations
    if selected_teams:
        # EPA trends over time
        st.subheader("EPA Trends Over Time")
        
        fig_trends = px.line(
            filtered_data, 
            x='week', 
            y='total_epa', 
            color='team',
            title="Total EPA by Week",
            hover_data=['epa_rank']
        )
        fig_trends.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="League Average")
        st.plotly_chart(fig_trends, use_container_width=True)
        
        # Running average EPA
        st.subheader("Season EPA Progression")
        
        fig_running = px.line(
            filtered_data, 
            x='week', 
            y='running_avg_epa', 
            color='team',
            title="Running Average EPA (Season-to-Date)",
        )
        st.plotly_chart(fig_running, use_container_width=True)
        
        # Pass vs Rush EPA comparison
        st.subheader("Offensive EPA Breakdown")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Pass EPA trends
            fig_pass = px.line(
                filtered_data, 
                x='week', 
                y='pass_epa', 
                color='team',
                title="Pass EPA Trends"
            )
            fig_pass.add_hline(y=0, line_dash="dash", line_color="gray")
            st.plotly_chart(fig_pass, use_container_width=True)
        
        with col2:
            # Rush EPA trends
            fig_rush = px.line(
                filtered_data, 
                x='week', 
                y='rush_epa', 
                color='team',
                title="Rush EPA Trends"
            )
            fig_rush.add_hline(y=0, line_dash="dash", line_color="gray")
            st.plotly_chart(fig_rush, use_container_width=True)
        
        # EPA correlation analysis
        st.subheader("Pass vs Rush EPA Relationship")
        
        # Aggregate by team for correlation
        team_summary = filtered_data.groupby('team').agg({
            'pass_epa': 'mean',
            'rush_epa': 'mean',
            'total_epa': 'mean',
            'wins': 'sum'
        }).reset_index()
        
        fig_correlation = px.scatter(
            team_summary,
            x='pass_epa',
            y='rush_epa',
            color='total_epa',
            size='wins',
            hover_data=['team'],
            title="Pass EPA vs Rush EPA (Team Averages)",
            labels={'pass_epa': 'Avg Pass EPA', 'rush_epa': 'Avg Rush EPA'}
        )
        fig_correlation.add_hline(y=0, line_dash="dash", line_color="gray")
        fig_correlation.add_vline(x=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig_correlation, use_container_width=True)
        
        # Weekly rankings heatmap
        st.subheader("Weekly EPA Rankings")
        
        # Create rankings matrix
        rankings_pivot = filtered_data.pivot(index='team', columns='week', values='epa_rank')
        
        fig_heatmap = go.Figure(data=go.Heatmap(
            z=rankings_pivot.values,
            x=rankings_pivot.columns,
            y=rankings_pivot.index,
            colorscale='RdYlGn_r',
            colorbar=dict(title="EPA Rank"),
            hoverongaps=False
        ))
        
        fig_heatmap.update_layout(
            title="EPA Rankings by Week (Lower is Better)",
            xaxis_title="Week",
            yaxis_title="Team"
        )
        st.plotly_chart(fig_heatmap, use_container_width=True)
        
        # Team comparison table
        st.subheader("Team Performance Summary")
        
        summary_stats = filtered_data.groupby('team').agg({
            'total_epa': ['mean', 'std', 'max', 'min'],
            'pass_epa': 'mean',
            'rush_epa': 'mean',
            'epa_rank': 'mean',
            'wins': 'sum',
            'losses': 'sum'
        }).round(3)
        
        summary_stats.columns = ['Avg EPA', 'EPA Std', 'Max EPA', 'Min EPA', 'Avg Pass EPA', 'Avg Rush EPA', 'Avg Rank', 'Wins', 'Losses']
        summary_stats['Win %'] = (summary_stats['Wins'] / (summary_stats['Wins'] + summary_stats['Losses']) * 100).round(1)
        
        # Sort by average EPA
        summary_stats = summary_stats.sort_values('Avg EPA', ascending=False)
        
        st.dataframe(summary_stats, use_container_width=True)
        
    else:
        st.info("Please select at least one team to display analysis.")

if __name__ == "__main__":
    show_team_analysis()