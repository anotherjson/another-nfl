"""
Fantasy Football Dashboard

Advanced fantasy analytics with projections and matchup analysis.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import os
import numpy as np

def show_fantasy_dashboard():
    """Show fantasy football analytics dashboard"""
    
    st.title("🏈 Fantasy Football Analytics")
    
    # Database connection
    conn = duckdb.connect(os.getenv("NFL_DATA_PATH", "/data") + "/nfl_analytics.duckdb", read_only=True)
    
    # Load fantasy data
    fantasy_query = """
    WITH player_fantasy AS (
        SELECT 
            player_name,
            position,
            team,
            week,
            season,
            fantasy_points_ppr,
            fantasy_points_standard,
            targets,
            receptions,
            receiving_yards,
            rushing_yards,
            rushing_attempts,
            passing_yards,
            passing_touchdowns,
            receiving_touchdowns,
            rushing_touchdowns,
            ROW_NUMBER() OVER (PARTITION BY week, position ORDER BY fantasy_points_ppr DESC) as position_rank
        FROM int_player_weekly_stats
        WHERE season = 2023 AND fantasy_points_ppr > 0
    )
    SELECT * FROM player_fantasy
    ORDER BY week, position, fantasy_points_ppr DESC
    """
    
    fantasy_data = conn.execute(fantasy_query).df()
    
    if fantasy_data.empty:
        st.warning("No fantasy data available. Make sure dbt models have been run.")
        return
    
    # Sidebar filters
    st.sidebar.subheader("Fantasy Filters")
    
    # Position selector
    positions = ['All'] + sorted(fantasy_data['position'].unique().tolist())
    selected_position = st.sidebar.selectbox("Position", positions)
    
    # Week range
    min_week = int(fantasy_data['week'].min())
    max_week = int(fantasy_data['week'].max())
    week_range = st.sidebar.slider("Week Range", min_week, max_week, (min_week, max_week))
    
    # Minimum games filter
    min_games = st.sidebar.slider("Minimum Games", 1, 18, 4)
    
    # Filter data
    filtered_data = fantasy_data[
        (fantasy_data['week'] >= week_range[0]) & 
        (fantasy_data['week'] <= week_range[1])
    ]
    
    if selected_position != 'All':
        filtered_data = filtered_data[filtered_data['position'] == selected_position]
    
    # Calculate player stats
    player_stats = filtered_data.groupby(['player_name', 'position', 'team']).agg({
        'fantasy_points_ppr': ['mean', 'std', 'sum', 'count', 'max'],
        'targets': 'sum',
        'receptions': 'sum',
        'receiving_yards': 'sum',
        'rushing_yards': 'sum',
        'passing_yards': 'sum',
        'position_rank': 'mean'
    }).round(2)
    
    player_stats.columns = ['Avg PPR', 'Std Dev', 'Total PPR', 'Games', 'Best Game', 
                           'Total Targets', 'Total Rec', 'Rec Yards', 'Rush Yards', 
                           'Pass Yards', 'Avg Rank']
    
    # Filter by minimum games
    player_stats = player_stats[player_stats['Games'] >= min_games]
    
    if player_stats.empty:
        st.warning("No players meet the selected criteria.")
        return
    
    # Calculate additional metrics
    player_stats['PPG'] = player_stats['Total PPR'] / player_stats['Games']
    player_stats['Consistency'] = (player_stats['Avg PPR'] / player_stats['Std Dev']).replace([np.inf, -np.inf], 0)
    player_stats['Ceiling'] = player_stats['Best Game']
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        top_scorer = player_stats['Total PPR'].idxmax()
        top_score = player_stats.loc[top_scorer, 'Total PPR']
        st.metric("Top Scorer", f"{top_score:.1f}", top_scorer[0])
    
    with col2:
        most_consistent = player_stats['Consistency'].idxmax()
        consistency_score = player_stats.loc[most_consistent, 'Consistency']
        st.metric("Most Consistent", f"{consistency_score:.1f}", most_consistent[0])
    
    with col3:
        total_players = len(player_stats)
        st.metric("Players Analyzed", total_players)
    
    with col4:
        avg_ppg = player_stats['PPG'].mean()
        st.metric("Avg PPG", f"{avg_ppg:.1f}")
    
    # Tab layout
    tab1, tab2, tab3, tab4 = st.tabs(["Player Rankings", "Consistency Analysis", "Matchup Analysis", "Projections"])
    
    with tab1:
        show_player_rankings(player_stats, filtered_data)
    
    with tab2:
        show_consistency_analysis(player_stats)
    
    with tab3:
        show_matchup_analysis(filtered_data)
    
    with tab4:
        show_projections(player_stats, filtered_data)

def show_player_rankings(player_stats, game_data):
    """Show player rankings and performance"""
    
    st.subheader("Player Rankings")
    
    # Sorting options
    sort_options = ['Total PPR', 'PPG', 'Consistency', 'Ceiling']
    sort_by = st.selectbox("Sort by:", sort_options)
    
    # Top performers
    top_players = player_stats.nlargest(20, sort_by).reset_index()
    
    # Visualization
    fig = px.bar(
        top_players.head(15),
        x=sort_by,
        y='player_name',
        color='position',
        title=f"Top 15 Players by {sort_by}",
        orientation='h'
    )
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    st.subheader("Detailed Player Statistics")
    
    display_cols = ['PPG', 'Total PPR', 'Games', 'Consistency', 'Ceiling', 'Avg Rank']
    st.dataframe(
        player_stats[display_cols].sort_values('PPG', ascending=False),
        use_container_width=True
    )

def show_consistency_analysis(player_stats):
    """Show player consistency analysis"""
    
    st.subheader("Consistency vs Ceiling Analysis")
    
    # Scatter plot of consistency vs ceiling
    fig = px.scatter(
        player_stats.reset_index(),
        x='Consistency',
        y='Ceiling',
        color='position',
        size='Games',
        hover_data=['player_name', 'team', 'PPG'],
        title="Player Consistency vs Ceiling",
        labels={'Consistency': 'Consistency Score', 'Ceiling': 'Best Single Game'}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Weekly variance analysis
    st.subheader("Weekly Performance Variance")
    
    # Select top players for variance analysis
    top_consistent = player_stats.nlargest(10, 'Consistency').reset_index()
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_variance = px.bar(
            top_consistent,
            x='Std Dev',
            y='player_name',
            title="Lowest Standard Deviation (Most Consistent)",
            orientation='h'
        )
        fig_variance.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_variance, use_container_width=True)
    
    with col2:
        fig_ceiling = px.bar(
            top_consistent,
            x='Ceiling',
            y='player_name',
            color='position',
            title="Best Single Game Performance",
            orientation='h'
        )
        fig_ceiling.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_ceiling, use_container_width=True)

def show_matchup_analysis(game_data):
    """Show matchup analysis"""
    
    st.subheader("Matchup Analysis")
    
    # Team-based analysis
    team_defense = game_data.groupby(['team']).agg({
        'fantasy_points_ppr': 'mean',
        'targets': 'mean',
        'receptions': 'mean',
        'receiving_yards': 'mean'
    }).round(2)
    
    team_defense.columns = ['Avg PPR Allowed', 'Avg Targets', 'Avg Receptions', 'Avg Rec Yards']
    team_defense = team_defense.sort_values('Avg PPR Allowed', ascending=False)
    
    st.subheader("Fantasy Points Allowed by Team Defense")
    
    fig_defense = px.bar(
        team_defense.reset_index(),
        x='team',
        y='Avg PPR Allowed',
        title="Average Fantasy Points Allowed by Team",
        color='Avg PPR Allowed',
        color_continuous_scale='RdYlGn_r'
    )
    fig_defense.update_xaxes(tickangle=45)
    st.plotly_chart(fig_defense, use_container_width=True)
    
    # Weekly trends
    st.subheader("Weekly Fantasy Trends")
    
    weekly_avg = game_data.groupby('week')['fantasy_points_ppr'].mean().reset_index()
    
    fig_weekly = px.line(
        weekly_avg,
        x='week',
        y='fantasy_points_ppr',
        title="Average Fantasy Points by Week",
        labels={'fantasy_points_ppr': 'Avg Fantasy Points'}
    )
    st.plotly_chart(fig_weekly, use_container_width=True)

def show_projections(player_stats, game_data):
    """Show fantasy projections"""
    
    st.subheader("Fantasy Projections")
    
    # Simple projection model based on recent performance
    recent_weeks = 4
    latest_week = game_data['week'].max()
    recent_data = game_data[game_data['week'] > (latest_week - recent_weeks)]
    
    if not recent_data.empty:
        projections = recent_data.groupby(['player_name', 'position', 'team']).agg({
            'fantasy_points_ppr': 'mean',
            'targets': 'mean',
            'receptions': 'mean',
            'receiving_yards': 'mean'
        }).round(2)
        
        projections.columns = ['Projected PPR', 'Projected Targets', 'Projected Receptions', 'Projected Yards']
        
        # Add confidence based on consistency
        player_consistency = player_stats['Consistency'].to_dict()
        projections['Confidence'] = projections.index.map(lambda x: player_consistency.get(x, 0))
        
        # Top projections
        top_projections = projections.nlargest(15, 'Projected PPR').reset_index()
        
        fig_proj = px.bar(
            top_projections,
            x='Projected PPR',
            y='player_name',
            color='Confidence',
            title=f"Next Week Projections (Based on Last {recent_weeks} Weeks)",
            orientation='h'
        )
        fig_proj.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_proj, use_container_width=True)
        
        # Projections table
        st.dataframe(
            projections.sort_values('Projected PPR', ascending=False).head(20),
            use_container_width=True
        )
    else:
        st.warning("Insufficient recent data for projections")
    
    # Season outlook
    st.subheader("Season Outlook")
    
    # Remaining games estimation (simplified)
    remaining_games = 18 - game_data.groupby('player_name')['week'].nunique()
    
    season_projections = player_stats.copy()
    season_projections['Remaining Games'] = season_projections.index.map(
        lambda x: remaining_games.get(x[0], 0)
    )
    season_projections['Projected Season Total'] = (
        season_projections['Total PPR'] + 
        (season_projections['PPG'] * season_projections['Remaining Games'])
    ).round(1)
    
    top_season = season_projections.nlargest(15, 'Projected Season Total').reset_index()
    
    fig_season = px.bar(
        top_season,
        x='Projected Season Total',
        y='player_name',
        color='position',
        title="Projected Season Totals",
        orientation='h'
    )
    fig_season.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig_season, use_container_width=True)

if __name__ == "__main__":
    show_fantasy_dashboard()