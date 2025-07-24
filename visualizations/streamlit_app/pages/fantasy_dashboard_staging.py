"""Fantasy Football Dashboard - dbt Staging Models Integration"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from typing import Optional, List, Dict
from datetime import datetime

# Import our new services and data models
import sys
sys.path.append('..')
from services.staging_service import get_staging_service
from data_models import PositionGroup, FantasyTier, PlayerWeeklyStats
from utils.dagster_monitor import get_pipeline_health_summary

# Configure page
st.set_page_config(
    page_title="Fantasy Football Analytics - Staging Models",
    page_icon="🏈",
    layout="wide"
)


def show_pipeline_status():
    """Show pipeline health status."""
    health_summary = get_pipeline_health_summary()
    
    if health_summary["status"] == "UNAVAILABLE":
        st.info("⚠️ Dagster monitoring unavailable - using direct dbt execution")
    else:
        pipeline_health = health_summary["pipeline_health"]
        
        if pipeline_health["overall_health"] == "HEALTHY":
            st.success(f"✅ Pipeline: {pipeline_health['fresh_assets']}/{pipeline_health['total_assets']} assets fresh")
        else:
            st.warning(f"⚠️ Pipeline: {pipeline_health['overall_health']} status")


@st.cache_data(ttl=600)  # 10 minute cache
def calculate_player_fantasy_stats(players: List[PlayerWeeklyStats], min_games: int = 1) -> pd.DataFrame:
    """Calculate comprehensive fantasy statistics from player data."""
    if not players:
        return pd.DataFrame()
    
    # Group players by player_id and calculate stats
    player_stats = {}
    
    for player in players:
        key = (player.player_id, player.player_display_name, player.position, player.team)
        
        if key not in player_stats:
            player_stats[key] = {
                'games': 0,
                'total_fantasy_ppr': 0,
                'fantasy_scores': [],
                'total_targets': 0,
                'total_receptions': 0,
                'total_receiving_yards': 0,
                'total_rushing_yards': 0,
                'total_passing_yards': 0,
                'total_receiving_tds': 0,
                'total_rushing_tds': 0,
                'total_passing_tds': 0,
                'position_group': player.position_group,
                'fantasy_tiers': []
            }
        
        stats = player_stats[key]
        stats['games'] += 1
        
        # Fantasy points
        fantasy_ppr = player.fantasy_points_ppr or 0
        stats['total_fantasy_ppr'] += fantasy_ppr
        stats['fantasy_scores'].append(fantasy_ppr)
        stats['fantasy_tiers'].append(player.fantasy_tier)
        
        # Volume stats
        stats['total_targets'] += player.targets or 0
        stats['total_receptions'] += player.receptions or 0
        stats['total_receiving_yards'] += player.receiving_yards or 0
        stats['total_rushing_yards'] += player.rushing_yards or 0
        stats['total_passing_yards'] += player.passing_yards or 0
        stats['total_receiving_tds'] += player.receiving_tds or 0
        stats['total_rushing_tds'] += player.rushing_tds or 0
        stats['total_passing_tds'] += player.passing_tds or 0
    
    # Convert to DataFrame
    processed_stats = []
    for (player_id, name, position, team), stats in player_stats.items():
        if stats['games'] >= min_games:
            fantasy_scores = stats['fantasy_scores']
            
            processed_stats.append({
                'player_id': player_id,
                'player_name': name,
                'position': position,
                'team': team,
                'position_group': stats['position_group'].value,
                'games': stats['games'],
                'total_fantasy_ppr': stats['total_fantasy_ppr'],
                'avg_fantasy_ppr': stats['total_fantasy_ppr'] / stats['games'],
                'std_dev_fantasy': np.std(fantasy_scores) if len(fantasy_scores) > 1 else 0,
                'max_fantasy': max(fantasy_scores) if fantasy_scores else 0,
                'min_fantasy': min(fantasy_scores) if fantasy_scores else 0,
                'total_targets': stats['total_targets'],
                'total_receptions': stats['total_receptions'],
                'total_receiving_yards': stats['total_receiving_yards'],
                'total_rushing_yards': stats['total_rushing_yards'],
                'total_passing_yards': stats['total_passing_yards'],
                'total_touchdowns': (stats['total_receiving_tds'] + 
                                   stats['total_rushing_tds'] + 
                                   stats['total_passing_tds']),
                'consistency_score': (stats['total_fantasy_ppr'] / stats['games']) / np.std(fantasy_scores) 
                                   if len(fantasy_scores) > 1 and np.std(fantasy_scores) > 0 else 0
            })
    
    return pd.DataFrame(processed_stats)


def main():
    """Main fantasy dashboard"""
    st.title("🏈 Fantasy Football Analytics")
    st.markdown("**Powered by dbt Staging Models & Dagster Orchestration**")
    
    # Show pipeline status
    show_pipeline_status()
    
    # Sidebar filters
    st.sidebar.header("🗓️ Filters")
    
    # Season filter
    current_year = 2024
    available_seasons = list(range(2018, current_year + 1))
    selected_season = st.sidebar.selectbox(
        "Season", 
        available_seasons[::-1],  # Recent first
        index=0
    )
    
    # Position filter
    position_options = ["All"] + [pg.value for pg in PositionGroup]
    selected_position = st.sidebar.selectbox("Position Group", position_options)
    position_filter = None
    if selected_position != "All":
        position_filter = PositionGroup(selected_position)
    
    # Week range filter
    week_start = st.sidebar.slider("Start Week", 1, 18, 1)
    week_end = st.sidebar.slider("End Week", 1, 18, 18)
    
    # Minimum games filter
    min_games = st.sidebar.slider("Minimum Games", 1, 18, 4)
    
    # Data freshness indicator
    service = get_staging_service()
    health_status = service.get_dashboard_health_status()
    
    if health_status['status'] != 'HEALTHY':
        st.warning(f"⚠️ Data Status: {health_status['message']}")
    
    # Load player data
    with st.spinner("Loading fantasy data from staging models..."):
        all_players = []
        
        # Load week by week to avoid memory issues
        for week in range(week_start, week_end + 1):
            week_players = service.get_player_stats(
                season=selected_season,
                week=week,
                position_group=position_filter,
                limit=500
            )
            all_players.extend(week_players)
        
        if not all_players:
            st.error("No player data found for selected filters")
            return
    
    # Calculate fantasy statistics
    fantasy_stats_df = calculate_player_fantasy_stats(all_players, min_games)
    
    if fantasy_stats_df.empty:
        st.warning("No players meet the minimum games criteria")
        return
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        top_scorer = fantasy_stats_df.loc[fantasy_stats_df['total_fantasy_ppr'].idxmax()]
        st.metric("Top Scorer", f"{top_scorer['total_fantasy_ppr']:.1f}", top_scorer['player_name'])
    
    with col2:
        most_consistent = fantasy_stats_df.loc[fantasy_stats_df['consistency_score'].idxmax()]
        st.metric("Most Consistent", f"{most_consistent['consistency_score']:.1f}", most_consistent['player_name'])
    
    with col3:
        st.metric("Players Analyzed", len(fantasy_stats_df))
    
    with col4:
        avg_ppg = fantasy_stats_df['avg_fantasy_ppr'].mean()
        st.metric("Avg PPG", f"{avg_ppg:.1f}")
    
    # Tab layout
    tab1, tab2, tab3, tab4 = st.tabs(["Player Rankings", "Consistency Analysis", "Position Analysis", "Weekly Trends"])
    
    with tab1:
        show_player_rankings(fantasy_stats_df)
    
    with tab2:
        show_consistency_analysis(fantasy_stats_df)
    
    with tab3:
        show_position_analysis(all_players, fantasy_stats_df)
    
    with tab4:
        show_weekly_trends(all_players, selected_season)


def show_player_rankings(fantasy_stats_df: pd.DataFrame):
    """Show player rankings and performance."""
    st.subheader("🏆 Player Rankings")
    
    # Sorting options
    sort_options = {
        'Total Fantasy Points': 'total_fantasy_ppr',
        'Average PPG': 'avg_fantasy_ppr', 
        'Consistency Score': 'consistency_score',
        'Best Game': 'max_fantasy'
    }
    
    sort_by_display = st.selectbox("Sort by:", list(sort_options.keys()))
    sort_by = sort_options[sort_by_display]
    
    # Top performers
    top_players = fantasy_stats_df.nlargest(20, sort_by).copy()
    
    # Visualization
    fig = px.bar(
        top_players.head(15),
        x=sort_by,
        y='player_name',
        color='position_group',
        title=f"Top 15 Players by {sort_by_display}",
        orientation='h',
        hover_data=['team', 'games']
    )
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    st.subheader("📊 Detailed Player Statistics")
    
    # Format display data
    display_data = []
    for _, player in fantasy_stats_df.sort_values('avg_fantasy_ppr', ascending=False).head(25).iterrows():
        display_data.append({
            'Player': player['player_name'],
            'Position': player['position'],
            'Team': player['team'],
            'Games': int(player['games']),
            'Total PPR': f"{player['total_fantasy_ppr']:.1f}",
            'Avg PPG': f"{player['avg_fantasy_ppr']:.1f}",
            'Best Game': f"{player['max_fantasy']:.1f}",
            'Consistency': f"{player['consistency_score']:.2f}",
            'Total Yards': int(player['total_receiving_yards'] + player['total_rushing_yards'] + player['total_passing_yards']),
            'Total TDs': int(player['total_touchdowns'])
        })
    
    st.dataframe(pd.DataFrame(display_data), use_container_width=True)


def show_consistency_analysis(fantasy_stats_df: pd.DataFrame):
    """Show player consistency analysis."""
    st.subheader("📈 Consistency vs Ceiling Analysis")
    
    # Scatter plot of consistency vs ceiling
    fig = px.scatter(
        fantasy_stats_df,
        x='consistency_score',
        y='max_fantasy',
        color='position_group',
        size='games',
        hover_data=['player_name', 'team', 'avg_fantasy_ppr'],
        title="Player Consistency vs Best Single Game",
        labels={'consistency_score': 'Consistency Score', 'max_fantasy': 'Best Single Game PPR'}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Weekly variance analysis
    col1, col2 = st.columns(2)
    
    with col1:
        # Most consistent players (lowest standard deviation)
        most_consistent = fantasy_stats_df.nsmallest(10, 'std_dev_fantasy')
        
        fig_consistent = px.bar(
            most_consistent,
            x='std_dev_fantasy',
            y='player_name',
            title="Most Consistent Players (Lowest Std Dev)",
            orientation='h',
            color='position_group'
        )
        fig_consistent.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_consistent, use_container_width=True)
    
    with col2:
        # Highest ceiling players
        highest_ceiling = fantasy_stats_df.nlargest(10, 'max_fantasy')
        
        fig_ceiling = px.bar(
            highest_ceiling,
            x='max_fantasy',
            y='player_name',
            title="Highest Single Game Performance",
            orientation='h',
            color='position_group'
        )
        fig_ceiling.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_ceiling, use_container_width=True)


def show_position_analysis(all_players: List[PlayerWeeklyStats], fantasy_stats_df: pd.DataFrame):
    """Show position-based fantasy analysis."""
    st.subheader("🎯 Position Analysis")
    
    # Position group distribution
    col1, col2 = st.columns(2)
    
    with col1:
        # Average PPG by position
        pos_avg = fantasy_stats_df.groupby('position_group')['avg_fantasy_ppr'].mean().sort_values(ascending=False)
        
        fig_pos_avg = px.bar(
            x=pos_avg.values,
            y=pos_avg.index,
            orientation='h',
            title="Average PPG by Position Group",
            color=pos_avg.values,
            color_continuous_scale='viridis'
        )
        st.plotly_chart(fig_pos_avg, use_container_width=True)
    
    with col2:
        # Position group player count
        pos_counts = fantasy_stats_df['position_group'].value_counts()
        
        fig_pos_count = px.pie(
            values=pos_counts.values,
            names=pos_counts.index,
            title="Players by Position Group"
        )
        st.plotly_chart(fig_pos_count, use_container_width=True)
    
    # Fantasy tier distribution
    st.subheader("Fantasy Performance Tiers")
    
    # Count tiers across all player games
    tier_counts = {}
    for player in all_players:
        tier = player.fantasy_tier.value
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
    
    fig_tiers = px.bar(
        x=list(tier_counts.keys()),
        y=list(tier_counts.values()),
        title="Distribution of Fantasy Performance Tiers",
        color=list(tier_counts.values()),
        color_continuous_scale='RdYlGn'
    )
    fig_tiers.update_xaxes(tickangle=45)
    st.plotly_chart(fig_tiers, use_container_width=True)
    
    # Top players by position
    st.subheader("Top Players by Position Group")
    
    position_groups = fantasy_stats_df['position_group'].unique()
    selected_pos_group = st.selectbox("Select Position Group", position_groups)
    
    pos_players = fantasy_stats_df[fantasy_stats_df['position_group'] == selected_pos_group].nlargest(10, 'avg_fantasy_ppr')
    
    if not pos_players.empty:
        fig_pos_top = px.bar(
            pos_players,
            x='avg_fantasy_ppr',
            y='player_name',
            title=f"Top {selected_pos_group} Players by Average PPG",
            orientation='h',
            hover_data=['team', 'games']
        )
        fig_pos_top.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_pos_top, use_container_width=True)


def show_weekly_trends(all_players: List[PlayerWeeklyStats], season: int):
    """Show weekly fantasy trends."""
    st.subheader("📅 Weekly Fantasy Trends")
    
    # Calculate weekly averages
    weekly_stats = {}
    for player in all_players:
        week = player.week
        if week not in weekly_stats:
            weekly_stats[week] = {
                'total_fantasy': 0,
                'player_count': 0,
                'position_groups': {}
            }
        
        weekly_stats[week]['total_fantasy'] += player.fantasy_points_ppr or 0
        weekly_stats[week]['player_count'] += 1
        
        pos_group = player.position_group.value
        if pos_group not in weekly_stats[week]['position_groups']:
            weekly_stats[week]['position_groups'][pos_group] = {'total': 0, 'count': 0}
        
        weekly_stats[week]['position_groups'][pos_group]['total'] += player.fantasy_points_ppr or 0
        weekly_stats[week]['position_groups'][pos_group]['count'] += 1
    
    # Convert to DataFrame for plotting
    weekly_data = []
    for week, stats in weekly_stats.items():
        if stats['player_count'] > 0:
            weekly_data.append({
                'week': week,
                'avg_fantasy_ppr': stats['total_fantasy'] / stats['player_count'],
                'total_players': stats['player_count']
            })
    
    if weekly_data:
        weekly_df = pd.DataFrame(weekly_data).sort_values('week')
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Average fantasy points by week
            fig_weekly = px.line(
                weekly_df,
                x='week',
                y='avg_fantasy_ppr',
                title=f"Average Fantasy Points by Week ({season})",
                markers=True
            )
            st.plotly_chart(fig_weekly, use_container_width=True)
        
        with col2:
            # Total active players by week
            fig_players = px.bar(
                weekly_df,
                x='week',
                y='total_players',
                title="Active Players by Week"
            )
            st.plotly_chart(fig_players, use_container_width=True)
    
    # Weekly position group trends
    st.subheader("Position Group Weekly Trends")
    
    # Create position group weekly data
    pos_weekly_data = []
    for week, stats in weekly_stats.items():
        for pos_group, pos_stats in stats['position_groups'].items():
            if pos_stats['count'] > 0:
                pos_weekly_data.append({
                    'week': week,
                    'position_group': pos_group,
                    'avg_fantasy_ppr': pos_stats['total'] / pos_stats['count']
                })
    
    if pos_weekly_data:
        pos_weekly_df = pd.DataFrame(pos_weekly_data)
        
        fig_pos_weekly = px.line(
            pos_weekly_df,
            x='week',
            y='avg_fantasy_ppr',
            color='position_group',
            title="Average Fantasy Points by Position Group and Week",
            markers=True
        )
        st.plotly_chart(fig_pos_weekly, use_container_width=True)
    
    # Data lineage information
    with st.expander("📋 Data Lineage & Quality"):
        st.write("**Data Sources:**")
        st.write("- Player Stats: `stg_weekly` (dbt staging model)")
        st.write("- Pipeline: NFL API → Dagster Assets → dbt Staging → Dashboard")
        
        service = get_staging_service()
        freshness_summary = service.get_data_freshness_summary()
        
        st.write("**Model Status:**")
        if 'stg_weekly' in freshness_summary:
            status = freshness_summary['stg_weekly']['dbt_status']
            status_icon = '✅' if status['is_fresh'] else ('❌' if status['error'] else '⚠️')
            st.write(f"- stg_weekly: {status_icon} {status['row_count'] or 0} rows")


if __name__ == "__main__":
    main()