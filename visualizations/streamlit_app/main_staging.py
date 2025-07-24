"""
NFL Analytics Streamlit Dashboard - dbt Staging Models Integration

Production-ready dashboard using dbt staging models with Dagster monitoring,
intelligent caching, and comprehensive error handling. Full migration from
parquet-direct queries to staging model integration.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import Optional, List

# Import our new services and data models
from services.staging_service import (
    get_staging_service, 
    load_dashboard_data,
    load_team_analysis_data,
    load_player_analysis_data
)
from data_models import (
    PositionGroup, Conference, FantasyTier, GameCompetitiveness,
    TeamInfo, PlayerWeeklyStats, GameSchedule
)
from utils.dagster_monitor import get_pipeline_health_summary

# Configure page
st.set_page_config(
    page_title="NFL Analytics Dashboard - Staging Models",
    page_icon="🏈",
    layout="wide",
    initial_sidebar_state="expanded"
)


def show_pipeline_health_header():
    """Show pipeline health status in header."""
    health_summary = get_pipeline_health_summary()
    
    if health_summary["status"] == "UNAVAILABLE":
        st.warning("⚠️ Dagster pipeline monitoring unavailable - using fallback mode")
    else:
        pipeline_health = health_summary["pipeline_health"]
        
        if pipeline_health["overall_health"] == "HEALTHY":
            st.success(f"✅ Pipeline Status: {pipeline_health['overall_health']} - {pipeline_health['fresh_assets']}/{pipeline_health['total_assets']} assets fresh")
        elif pipeline_health["overall_health"] == "DEGRADED":
            st.warning(f"⚠️ Pipeline Status: {pipeline_health['overall_health']} - {pipeline_health['fresh_assets']}/{pipeline_health['total_assets']} assets fresh")
        else:
            st.error(f"❌ Pipeline Status: {pipeline_health['overall_health']} - {pipeline_health['failed_assets']} failed assets")


def show_data_freshness_sidebar():
    """Show data freshness information in sidebar."""
    st.sidebar.header("📊 Data Status")
    
    service = get_staging_service()
    health_status = service.get_dashboard_health_status()
    
    # Overall health indicator
    if health_status['status'] == 'HEALTHY':
        st.sidebar.success(f"✅ {health_status['message']}")
    elif health_status['status'] == 'DEGRADED':
        st.sidebar.warning(f"⚠️ {health_status['message']}")
    else:
        st.sidebar.error(f"❌ {health_status['message']}")
    
    # Dagster status
    if health_status['dagster_available']:
        st.sidebar.info("🔄 Dagster orchestration active")
    else:
        st.sidebar.info("🔧 Using direct dbt execution")
    
    # Model status details
    if st.sidebar.expander("Model Details"):
        freshness_summary = service.get_data_freshness_summary()
        
        for model_name, status in freshness_summary.items():
            dbt_status = status['dbt_status']
            
            if dbt_status['is_fresh']:
                st.sidebar.text(f"✅ {model_name}: {dbt_status['row_count']} rows")
            elif dbt_status['error']:
                st.sidebar.text(f"❌ {model_name}: Error")
            else:
                st.sidebar.text(f"⚠️ {model_name}: Stale")
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh All Models"):
        with st.spinner("Refreshing staging models..."):
            result = service.dbt_connector.refresh_all_staging_models()
            
            if result.success:
                st.sidebar.success(f"Refreshed {len(result.models_executed)} models")
                st.experimental_rerun()
            else:
                st.sidebar.error(f"Refresh failed: {result.error}")


def main():
    """Main dashboard application."""
    st.title("🏈 NFL Analytics Dashboard")
    st.markdown("**Powered by dbt Staging Models & Dagster Orchestration**")
    
    # Show pipeline health
    show_pipeline_health_header()
    
    # Sidebar with data status and navigation
    show_data_freshness_sidebar()
    
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Choose a page", [
        "Overview", 
        "Team Analysis", 
        "Player Stats",
        "Schedule Analysis",
        "System Health"
    ])
    
    # Season filter in sidebar
    st.sidebar.header("🗓️ Filters")
    current_year = datetime.now().year
    available_seasons = list(range(2018, current_year + 1))
    selected_season = st.sidebar.selectbox(
        "Season", 
        ["All"] + available_seasons[::-1],  # Reverse to show recent first
        index=1 if len(available_seasons) > 0 else 0
    )
    
    season_filter = selected_season if selected_season != "All" else None
    
    # Route to appropriate page
    if page == "Overview":
        show_overview(season_filter)
    elif page == "Team Analysis":
        show_team_analysis()
    elif page == "Player Stats":
        show_player_stats(season_filter)
    elif page == "Schedule Analysis":
        show_schedule_analysis(season_filter)
    elif page == "System Health":
        show_system_health()


def show_overview(season: Optional[int] = None):
    """Overview page with dashboard summary."""
    st.header("NFL Data Overview")
    
    # Load dashboard data
    with st.spinner("Loading dashboard data..."):
        dashboard_data = load_dashboard_data(season=season)
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("NFL Teams", len(dashboard_data['teams']))
        afc_count = len([t for t in dashboard_data['teams'] if t.team_conference == Conference.AFC])
        nfc_count = len(dashboard_data['teams']) - afc_count
        st.write(f"• AFC: {afc_count}")
        st.write(f"• NFC: {nfc_count}")
    
    with col2:
        st.metric("Top Players", len(dashboard_data['top_players']))
        if dashboard_data['top_players']:
            avg_fantasy = sum(p.fantasy_points_ppr or 0 for p in dashboard_data['top_players']) / len(dashboard_data['top_players'])
            st.write(f"Avg Fantasy: {avg_fantasy:.1f}")
    
    with col3:
        st.metric("Recent Games", len(dashboard_data['recent_games']))
        completed_games = [g for g in dashboard_data['recent_games'] if g.total_points is not None]
        if completed_games:
            avg_total_points = sum(g.total_points for g in completed_games) / len(completed_games)
            st.write(f"Avg Points: {avg_total_points:.1f}")
    
    with col4:
        health_status = dashboard_data['health_status']
        status_emoji = {"HEALTHY": "✅", "DEGRADED": "⚠️", "CRITICAL": "❌"}
        st.metric("System Health", health_status['status'])
        st.write(f"{status_emoji.get(health_status['status'], '❓')} {health_status['fresh_models']}/{health_status['total_models']} fresh")
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        # Conference distribution pie chart
        afc_teams = [t for t in dashboard_data['teams'] if t.team_conference == Conference.AFC]
        nfc_teams = [t for t in dashboard_data['teams'] if t.team_conference == Conference.NFC]
        
        fig = px.pie(
            values=[len(afc_teams), len(nfc_teams)],
            names=['AFC', 'NFC'],
            title="Teams by Conference",
            color_discrete_map={"AFC": "#FF6B6B", "NFC": "#4ECDC4"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Fantasy performance tiers
        if dashboard_data['top_players']:
            tier_counts = {}
            for player in dashboard_data['top_players']:
                tier = player.fantasy_tier.value
                tier_counts[tier] = tier_counts.get(tier, 0) + 1
            
            fig = px.bar(
                x=list(tier_counts.values()),
                y=list(tier_counts.keys()),
                orientation='h',
                title="Top Players by Fantasy Tier",
                color=list(tier_counts.values()),
                color_continuous_scale="viridis"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Top performers table
    if dashboard_data['top_players']:
        st.subheader("🌟 Top Fantasy Performers")
        
        # Convert to display format
        display_data = []
        for player in dashboard_data['top_players'][:10]:
            display_data.append({
                'Player': player.player_display_name,
                'Position': player.position,
                'Team': player.team,
                'Fantasy Points': f"{player.fantasy_points_ppr:.1f}" if player.fantasy_points_ppr else "0.0",
                'Total Yards': player.total_yards,
                'Total TDs': player.total_touchdowns,
                'Tier': player.fantasy_tier.value
            })
        
        st.dataframe(pd.DataFrame(display_data), use_container_width=True)
    
    # Data lineage information
    with st.expander("📋 Data Lineage & Quality"):
        st.write("**Data Pipeline Flow:**")
        st.write("NFL API → Dagster Raw Assets → dbt Staging Models → Dashboard")
        
        st.write("**Model Information:**")
        freshness_summary = get_staging_service().get_data_freshness_summary()
        
        lineage_data = []
        for model_name, status in freshness_summary.items():
            dbt_status = status['dbt_status']
            lineage_data.append({
                'Model': model_name,
                'Status': '✅ Fresh' if dbt_status['is_fresh'] else ('❌ Error' if dbt_status['error'] else '⚠️ Stale'),
                'Rows': dbt_status['row_count'] or 0,
                'Last Updated': dbt_status['last_run'][:19] if dbt_status['last_run'] else 'Never'
            })
        
        st.dataframe(pd.DataFrame(lineage_data), use_container_width=True)


def show_team_analysis():
    """Team analysis page."""
    st.header("🏈 Team Analysis")
    
    # Conference filter
    conference_filter = st.selectbox(
        "Conference", 
        ["All", "AFC", "NFC"]
    )
    
    selected_conference = None
    if conference_filter == "AFC":
        selected_conference = Conference.AFC
    elif conference_filter == "NFC":
        selected_conference = Conference.NFC
    
    # Load team data
    with st.spinner("Loading team data..."):
        team_data = load_team_analysis_data(selected_conference)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Conference distribution
        fig = px.pie(
            values=[len(team_data['afc_teams']), len(team_data['nfc_teams'])],
            names=['AFC', 'NFC'],
            title="Teams by Conference",
            color_discrete_map={"AFC": "#FF6B6B", "NFC": "#4ECDC4"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Division distribution
        division_counts = {}
        for team in team_data['teams']:
            division = team.team_division
            division_counts[division] = division_counts.get(division, 0) + 1
        
        fig = px.bar(
            x=list(division_counts.values()),
            y=list(division_counts.keys()),
            orientation='h',
            title="Teams by Division",
            color=list(division_counts.values()),
            color_continuous_scale="viridis"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Team details table
    st.subheader("Team Details")
    
    display_data = []
    for team in team_data['teams']:
        display_data.append({
            'Team': team.team_name,
            'Abbreviation': team.team_abbr,
            'Conference': team.team_conference.value,
            'Division': team.team_division,
            'Colors': f"{team.team_color or 'N/A'}, {team.team_color2 or 'N/A'}"
        })
    
    st.dataframe(pd.DataFrame(display_data), use_container_width=True)


def show_player_stats(season: Optional[int] = None):
    """Player statistics page."""
    st.header("👤 Player Statistics")
    
    # Filters
    col1, col2 = st.columns(2)
    
    with col1:
        position_filter = st.selectbox(
            "Position Group",
            ["All"] + [pg.value for pg in PositionGroup]
        )
        
        selected_position = None
        if position_filter != "All":
            selected_position = PositionGroup(position_filter)
    
    with col2:
        fantasy_tier_filter = st.selectbox(
            "Fantasy Tier",
            ["All"] + [ft.value for ft in FantasyTier]
        )
    
    # Load player data
    with st.spinner("Loading player data..."):
        player_data = load_player_analysis_data(season=season, position_group=selected_position)
    
    # Apply fantasy tier filter
    if fantasy_tier_filter != "All":
        selected_tier = FantasyTier(fantasy_tier_filter)
        filtered_players = [p for p in player_data['top_performers'] if p.fantasy_tier == selected_tier]
    else:
        filtered_players = player_data['top_performers']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Position group distribution
        if player_data['all_players']:
            position_counts = {}
            for player in player_data['all_players']:
                pos_group = player.position_group.value
                position_counts[pos_group] = position_counts.get(pos_group, 0) + 1
            
            fig = px.bar(
                x=list(position_counts.values()),
                y=list(position_counts.keys()),
                orientation='h',
                title="Players by Position Group"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Fantasy points distribution
        if filtered_players:
            fantasy_points = [p.fantasy_points_ppr or 0 for p in filtered_players]
            
            fig = px.histogram(
                x=fantasy_points,
                title="Fantasy Points Distribution",
                nbins=20
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Top performers table
    st.subheader("🌟 Top Performers")
    
    if filtered_players:
        display_data = []
        for player in filtered_players[:20]:
            display_data.append({
                'Player': player.player_display_name,
                'Position': player.position,
                'Team': player.team,
                'Season': player.season,
                'Week': player.week,
                'Fantasy Points': f"{player.fantasy_points_ppr:.1f}" if player.fantasy_points_ppr else "0.0",
                'Total Yards': player.total_yards,
                'TDs': player.total_touchdowns,
                'Tier': player.fantasy_tier.value
            })
        
        st.dataframe(pd.DataFrame(display_data), use_container_width=True)
    else:
        st.info("No players match the selected filters.")


def show_schedule_analysis(season: Optional[int] = None):
    """Schedule analysis page."""
    st.header("📅 Schedule Analysis")
    
    service = get_staging_service()
    
    # Load schedule data
    with st.spinner("Loading schedule data..."):
        games = service.get_game_schedules(season=season, limit=500)
        scoring_stats = service.get_scoring_statistics(season=season)
    
    # Key metrics
    if scoring_stats:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Average Total Points", f"{scoring_stats['avg_total_points']:.1f}")
        with col2:
            st.metric("Highest Scoring Game", scoring_stats['max_total_points'])
        with col3:
            st.metric("Lowest Scoring Game", scoring_stats['min_total_points'])
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Games by week
        week_counts = {}
        for game in games:
            week = game.week
            week_counts[week] = week_counts.get(week, 0) + 1
        
        if week_counts:
            weeks = sorted(week_counts.keys())
            counts = [week_counts[w] for w in weeks]
            
            fig = px.line(
                x=weeks,
                y=counts,
                title="Games per Week",
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Game competitiveness
        completed_games = [g for g in games if g.competitiveness is not None]
        if completed_games:
            comp_counts = {}
            for game in completed_games:
                comp = game.competitiveness.value
                comp_counts[comp] = comp_counts.get(comp, 0) + 1
            
            fig = px.pie(
                values=list(comp_counts.values()),
                names=list(comp_counts.keys()),
                title="Game Competitiveness"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Recent games table
    st.subheader("Recent Games")
    
    if games:
        display_data = []
        for game in games[:20]:
            display_data.append({
                'Season': game.season,
                'Week': game.week,
                'Home Team': game.home_team,
                'Away Team': game.away_team,
                'Home Score': game.home_score or 'TBD',
                'Away Score': game.away_score or 'TBD',
                'Winner': game.winner or 'TBD',
                'Total Points': game.total_points or 'TBD',
                'Competitiveness': game.competitiveness.value if game.competitiveness else 'TBD'
            })
        
        st.dataframe(pd.DataFrame(display_data), use_container_width=True)


def show_system_health():
    """System health and monitoring page."""
    st.header("🔧 System Health & Monitoring")
    
    service = get_staging_service()
    
    # Overall health status
    health_status = service.get_dashboard_health_status()
    
    if health_status['status'] == 'HEALTHY':
        st.success(f"✅ System Status: {health_status['message']}")
    elif health_status['status'] == 'DEGRADED':
        st.warning(f"⚠️ System Status: {health_status['message']}")
    else:
        st.error(f"❌ System Status: {health_status['message']}")
    
    # Pipeline health from Dagster
    pipeline_health = get_pipeline_health_summary()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 dbt Model Status")
        freshness_summary = service.get_data_freshness_summary()
        
        model_status_data = []
        for model_name, status in freshness_summary.items():
            dbt_status = status['dbt_status']
            
            model_status_data.append({
                'Model': model_name,
                'Status': '✅ Fresh' if dbt_status['is_fresh'] else ('❌ Error' if dbt_status['error'] else '⚠️ Stale'),
                'Rows': dbt_status['row_count'] or 0,
                'Last Run': dbt_status['last_run'][:19] if dbt_status['last_run'] else 'Never',
                'Error': dbt_status['error'] or 'None'
            })
        
        st.dataframe(pd.DataFrame(model_status_data), use_container_width=True)
    
    with col2:
        st.subheader("🔄 Dagster Pipeline Status")
        
        if pipeline_health["status"] == "AVAILABLE":
            pipeline_data = pipeline_health["pipeline_health"]
            
            st.write(f"**Overall Health:** {pipeline_data['overall_health']}")
            st.write(f"**Total Assets:** {pipeline_data['total_assets']}")
            st.write(f"**Materialized Assets:** {pipeline_data['materialized_assets']}")
            st.write(f"**Fresh Assets:** {pipeline_data['fresh_assets']}")
            st.write(f"**Failed Assets:** {pipeline_data['failed_assets']}")
            
            if pipeline_data['last_run_time']:
                st.write(f"**Last Run:** {pipeline_data['last_run_time'][:19]}")
        else:
            st.warning("Dagster pipeline monitoring unavailable")
    
    # Manual refresh controls
    st.subheader("🔄 Manual Controls")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Refresh All Staging Models"):
            with st.spinner("Refreshing all staging models..."):
                result = service.dbt_connector.refresh_all_staging_models()
                
                if result.success:
                    st.success(f"✅ Refreshed {len(result.models_executed)} models in {result.execution_time:.1f}s")
                else:
                    st.error(f"❌ Refresh failed: {result.error}")
    
    with col2:
        if st.button("Clear Cache"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("✅ Cache cleared")
    
    with col3:
        if st. button("System Info"):
            st.info(f"""
            **Dashboard Version:** dbt Staging Models Integration
            **Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            **Cache Status:** Active
            **Dagster Integration:** {health_status['dagster_available']}
            """)


if __name__ == "__main__":
    main()