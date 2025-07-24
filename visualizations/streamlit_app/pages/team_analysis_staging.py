"""NFL Team Analysis Dashboard Page - dbt Staging Models Integration"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Optional, List

# Import our new services and data models
import sys
sys.path.append('..')
from services.staging_service import get_staging_service, load_team_analysis_data
from data_models import TeamInfo, Conference
from utils.dagster_monitor import get_pipeline_health_summary

# Configure page
st.set_page_config(
    page_title="NFL Team Analysis - Staging Models",
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
def load_team_performance_data(season: Optional[int] = None) -> pd.DataFrame:
    """Load team performance data from staging models."""
    service = get_staging_service()
    
    # Get games from staging model
    games = service.get_game_schedules(season=season, limit=1000)
    
    # Calculate team performance metrics
    team_stats = {}
    
    for game in games:
        if game.home_score is None or game.away_score is None:
            continue  # Skip games without scores
        
        # Home team stats
        home_team = game.home_team
        if home_team not in team_stats:
            team_stats[home_team] = {
                'team': home_team,
                'games_played': 0,
                'wins': 0,
                'losses': 0,
                'points_for': 0,
                'points_against': 0
            }
        
        team_stats[home_team]['games_played'] += 1
        team_stats[home_team]['points_for'] += game.home_score
        team_stats[home_team]['points_against'] += game.away_score
        
        if game.home_score > game.away_score:
            team_stats[home_team]['wins'] += 1
        else:
            team_stats[home_team]['losses'] += 1
        
        # Away team stats
        away_team = game.away_team
        if away_team not in team_stats:
            team_stats[away_team] = {
                'team': away_team,
                'games_played': 0,
                'wins': 0,
                'losses': 0,
                'points_for': 0,
                'points_against': 0
            }
        
        team_stats[away_team]['games_played'] += 1
        team_stats[away_team]['points_for'] += game.away_score
        team_stats[away_team]['points_against'] += game.home_score
        
        if game.away_score > game.home_score:
            team_stats[away_team]['wins'] += 1
        else:
            team_stats[away_team]['losses'] += 1
    
    # Convert to DataFrame and calculate additional metrics
    performance_data = []
    for team_abbr, stats in team_stats.items():
        if stats['games_played'] > 0:
            performance_data.append({
                'team': team_abbr,
                'games_played': stats['games_played'],
                'wins': stats['wins'],
                'losses': stats['losses'],
                'win_percentage': stats['wins'] / stats['games_played'],
                'total_points_for': stats['points_for'],
                'total_points_against': stats['points_against'],
                'avg_points_for': stats['points_for'] / stats['games_played'],
                'avg_points_against': stats['points_against'] / stats['games_played'],
                'point_differential': stats['points_for'] - stats['points_against']
            })
    
    return pd.DataFrame(performance_data)


def main():
    """Main team analysis page"""
    st.title("🏈 NFL Team Analysis")
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
        ["All"] + available_seasons[::-1],  # Recent first
        index=1 if available_seasons else 0
    )
    season_filter = selected_season if selected_season != "All" else None
    
    # Conference filter
    selected_conference = st.sidebar.selectbox("Conference", ["All", "AFC", "NFC"])
    conference_filter = None
    if selected_conference == "AFC":
        conference_filter = Conference.AFC
    elif selected_conference == "NFC":
        conference_filter = Conference.NFC
    
    # Load data using our staging service
    with st.spinner("Loading team data from staging models..."):
        team_data = load_team_analysis_data(conference_filter)
        performance_df = load_team_performance_data(season_filter)
    
    # Apply division filter
    divisions = ["All"] + sorted(list(set(t.team_division for t in team_data['teams'])))
    selected_division = st.sidebar.selectbox("Division", divisions)
    
    filtered_teams = team_data['teams']
    if selected_division != "All":
        filtered_teams = [t for t in filtered_teams if t.team_division == selected_division]
    
    # Data freshness indicator
    service = get_staging_service()
    health_status = service.get_dashboard_health_status()
    
    if health_status['status'] != 'HEALTHY':
        st.warning(f"⚠️ Data Status: {health_status['message']}")
    
    # Overview metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Teams", len(filtered_teams))
    
    with col2:
        afc_count = len([t for t in filtered_teams if t.team_conference == Conference.AFC])
        st.metric("AFC Teams", afc_count)
    
    with col3:
        nfc_count = len([t for t in filtered_teams if t.team_conference == Conference.NFC])
        st.metric("NFC Teams", nfc_count)
    
    with col4:
        if not performance_df.empty:
            avg_wins = performance_df['wins'].mean()
            st.metric("Avg Wins", f"{avg_wins:.1f}")
        else:
            st.metric("Avg Wins", "N/A")
    
    # Visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        # Conference distribution using our data models
        afc_teams = [t for t in filtered_teams if t.team_conference == Conference.AFC]
        nfc_teams = [t for t in filtered_teams if t.team_conference == Conference.NFC]
        
        fig = px.pie(
            values=[len(afc_teams), len(nfc_teams)],
            names=['AFC', 'NFC'],
            title="Teams by Conference",
            color_discrete_map={"AFC": "#FF6B6B", "NFC": "#4ECDC4"}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Division distribution
        division_counts = {}
        for team in filtered_teams:
            division = team.team_division
            division_counts[division] = division_counts.get(division, 0) + 1
        
        if division_counts:
            fig = px.bar(
                x=list(division_counts.values()),
                y=list(division_counts.keys()),
                orientation='h',
                title="Teams by Division",
                color=list(division_counts.values()),
                color_continuous_scale="viridis"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Performance analysis
    if not performance_df.empty and season_filter:
        st.subheader(f"🏆 Team Performance - {season_filter} Season")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Win percentage distribution
            fig = px.histogram(
                performance_df,
                x='win_percentage',
                title="Win Percentage Distribution",
                nbins=12,
                color_discrete_sequence=['#1f77b4']
            )
            fig.update_layout(xaxis_title="Win Percentage", yaxis_title="Number of Teams")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Points scored vs allowed scatter plot
            fig = px.scatter(
                performance_df,
                x='avg_points_for',
                y='avg_points_against',
                hover_data=['team', 'wins', 'losses'],
                title="Average Points For vs Against",
                color='win_percentage',
                color_continuous_scale="RdYlGn",
                size='games_played'
            )
            fig.update_layout(
                xaxis_title="Average Points For",
                yaxis_title="Average Points Against"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Team performance table with enhanced data
        st.subheader(f"📊 {season_filter} Season Performance Details")
        
        if not performance_df.empty:
            # Create team lookup for enhanced display
            team_lookup = {t.team_abbr: t for t in filtered_teams}
            
            # Enhance performance data with team information
            display_data = []
            for _, row in performance_df.iterrows():
                team_abbr = row['team']
                team_info = team_lookup.get(team_abbr)
                
                if team_info:  # Only include teams in our filter
                    display_data.append({
                        'Team': team_info.team_name,
                        'Abbr': team_abbr,
                        'Conference': team_info.team_conference.value,
                        'Division': team_info.team_division,
                        'Wins': int(row['wins']),
                        'Losses': int(row['losses']),
                        'Win %': f"{row['win_percentage']:.3f}",
                        'Avg Points For': f"{row['avg_points_for']:.1f}",
                        'Avg Points Against': f"{row['avg_points_against']:.1f}",
                        'Point Diff': f"{row['point_differential']:+.0f}"
                    })
            
            if display_data:
                display_df = pd.DataFrame(display_data)
                # Sort by win percentage descending
                display_df = display_df.sort_values('Win %', ascending=False)
                st.dataframe(display_df, use_container_width=True)
            else:
                st.info("No performance data available for selected filters and season.")
    elif season_filter:
        st.info(f"No performance data available for {season_filter} season.")
    else:
        st.info("Select a specific season to view performance data.")
    
    # Team information section
    st.subheader("🏈 Team Information")
    
    # Team details table using our data models
    display_data = []
    for team in sorted(filtered_teams, key=lambda t: (t.team_conference.value, t.team_division, t.team_name)):
        display_data.append({
            'Team': team.team_name,
            'Abbreviation': team.team_abbr,
            'Conference': team.team_conference.value,
            'Division': team.team_division,
            'Primary Color': team.team_color or 'N/A',
            'Secondary Color': team.team_color2 or 'N/A',
            'Data Freshness': '✅ Fresh' if health_status['status'] == 'HEALTHY' else '⚠️ Stale'
        })
    
    if display_data:
        st.dataframe(pd.DataFrame(display_data), use_container_width=True)
    
    # Data lineage information
    with st.expander("📋 Data Lineage & Quality"):
        st.write("**Data Sources:**")
        st.write("- Team Information: `stg_team_desc` (dbt staging model)")
        st.write("- Performance Data: `stg_schedules` (calculated from game results)")
        st.write("- Pipeline: NFL API → Dagster Assets → dbt Staging → Dashboard")
        
        freshness_summary = service.get_data_freshness_summary()
        
        st.write("**Model Status:**")
        for model_name in ['stg_team_desc', 'stg_schedules']:
            if model_name in freshness_summary:
                status = freshness_summary[model_name]['dbt_status']
                status_icon = '✅' if status['is_fresh'] else ('❌' if status['error'] else '⚠️')
                st.write(f"- {model_name}: {status_icon} {status['row_count'] or 0} rows")


if __name__ == "__main__":
    main()