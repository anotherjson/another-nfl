"""
Business logic service layer for dbt staging model integration.

Provides high-level data access methods that combine the DbtStagingConnector
with data models and business logic for the Streamlit dashboard.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
import logging

from ..utils.dbt_staging_connector import DbtStagingConnector, get_dbt_connector
from ..utils.dagster_monitor import DagsterMonitor, get_dagster_monitor
from ..data_models import (
    TeamInfo, PlayerWeeklyStats, GameSchedule, PlayByPlayData,
    PositionGroup, Conference, FantasyTier, GameCompetitiveness,
    dataframe_to_models, filter_models
)


class StagingDataService:
    """
    Service layer for accessing and processing dbt staging model data.
    
    Provides business logic methods that combine staging data with
    Dagster monitoring and intelligent caching for optimal performance.
    """
    
    def __init__(self):
        self.dbt_connector = get_dbt_connector()
        self.dagster_monitor = get_dagster_monitor()
    
    # Team data methods
    @st.cache_data(ttl=1800)  # 30 minute cache for relatively static team data
    def get_all_teams(_self) -> List[TeamInfo]:
        """Get all NFL team information from staging model."""
        df = _self.dbt_connector.query_staging_model('stg_team_desc')
        return dataframe_to_models(df, TeamInfo)
    
    @st.cache_data(ttl=1800)
    def get_teams_by_conference(_self, conference: Conference) -> List[TeamInfo]:
        """Get teams filtered by conference."""
        teams = _self.get_all_teams()
        return filter_models(teams, team_conference=conference)
    
    @st.cache_data(ttl=1800)
    def get_teams_by_division(_self, division: str) -> List[TeamInfo]:
        """Get teams filtered by division."""
        teams = _self.get_all_teams()
        return filter_models(teams, team_division=division)
    
    def get_team_by_abbr(self, team_abbr: str) -> Optional[TeamInfo]:
        """Get a specific team by abbreviation."""
        teams = self.get_all_teams()
        matching_teams = filter_models(teams, team_abbr=team_abbr)
        return matching_teams[0] if matching_teams else None
    
    # Player data methods
    @st.cache_data(ttl=600)  # 10 minute cache for player stats
    def get_player_stats(_self, 
                        season: Optional[int] = None,
                        week: Optional[int] = None,
                        position_group: Optional[PositionGroup] = None,
                        team: Optional[str] = None,
                        limit: int = 1000) -> List[PlayerWeeklyStats]:
        """Get player weekly statistics with optional filters."""
        
        # Build filters dict for the connector
        filters = {}
        if season:
            filters['season'] = season
        if week:
            filters['week'] = week
        if team:
            filters['team'] = team
        
        df = _self.dbt_connector.query_staging_model(
            'stg_weekly', 
            filters=filters,
            limit=limit
        )
        
        players = dataframe_to_models(df, PlayerWeeklyStats)
        
        # Apply position group filter in Python (since it's a computed property)
        if position_group:
            players = [p for p in players if p.position_group == position_group]
        
        return players
    
    @st.cache_data(ttl=600)
    def get_top_fantasy_performers(_self,
                                  season: Optional[int] = None,
                                  position_group: Optional[PositionGroup] = None,
                                  limit: int = 20) -> List[PlayerWeeklyStats]:
        """Get top fantasy performers with optional filters."""
        players = _self.get_player_stats(season=season, position_group=position_group, limit=500)
        
    # New methods for additional staging models
    @st.cache_data(ttl=1800)  # 30 minute cache
    def get_injury_reports(_self,
                          season: Optional[int] = None,
                          week: Optional[int] = None,
                          team: Optional[str] = None,
                          limit: int = 1000) -> pd.DataFrame:
        """Get injury reports from staging model."""
        filters = {}
        if season:
            filters['season'] = season
        if week:
            filters['week'] = week
        if team:
            filters['team'] = team
            
        return _self.dbt_connector.query_staging_model(
            'stg_injuries',
            filters=filters,
            limit=limit
        )
    
    @st.cache_data(ttl=1800)
    def get_depth_charts(_self,
                        season: Optional[int] = None,
                        week: Optional[int] = None,
                        team: Optional[str] = None,
                        position: Optional[str] = None,
                        limit: int = 1000) -> pd.DataFrame:
        """Get depth chart data from staging model."""
        filters = {}
        if season:
            filters['season'] = season
        if week:
            filters['week'] = week
        if team:
            filters['team'] = team
        if position:
            filters['position'] = position
            
        return _self.dbt_connector.query_staging_model(
            'stg_depth_charts',
            filters=filters,
            limit=limit
        )
    
    @st.cache_data(ttl=600)  # 10 minute cache for snap counts
    def get_snap_counts(_self,
                       season: Optional[int] = None,
                       week: Optional[int] = None,
                       team: Optional[str] = None,
                       limit: int = 1000) -> pd.DataFrame:
        """Get snap count data from staging model."""
        filters = {}
        if season:
            filters['season'] = season
        if week:
            filters['week'] = week
        if team:
            filters['team'] = team
            
        return _self.dbt_connector.query_staging_model(
            'stg_snap_counts',
            filters=filters,
            limit=limit
        )
    
    @st.cache_data(ttl=600)
    def get_qbr_data(_self,
                    season: Optional[int] = None,
                    week: Optional[int] = None,
                    limit: int = 500) -> pd.DataFrame:
        """Get QBR data from staging model."""
        filters = {}
        if season:
            filters['season'] = season
        if week:
            filters['week'] = week
            
        return _self.dbt_connector.query_staging_model(
            'stg_qbr',
            filters=filters,
            limit=limit
        )
    
    @st.cache_data(ttl=1800)
    def get_ngs_data(_self,
                    season: Optional[int] = None,
                    week: Optional[int] = None,
                    position: Optional[str] = None,
                    limit: int = 1000) -> pd.DataFrame:
        """Get Next Gen Stats data from staging model."""
        filters = {}
        if season:
            filters['season'] = season
        if week:
            filters['week'] = week
        if position:
            filters['position'] = position
            
        return _self.dbt_connector.query_staging_model(
            'stg_ngs_data',
            filters=filters,
            limit=limit
        )
    
    @st.cache_data(ttl=3600)  # 1 hour cache for roster data
    def get_weekly_rosters(_self,
                          season: Optional[int] = None,
                          week: Optional[int] = None,
                          team: Optional[str] = None,
                          limit: int = 2000) -> pd.DataFrame:
        """Get weekly roster data from staging model."""
        filters = {}
        if season:
            filters['season'] = season
        if week:
            filters['week'] = week
        if team:
            filters['team'] = team
            
        return _self.dbt_connector.query_staging_model(
            'stg_weekly_rosters',
            filters=filters,
            limit=limit
        )
        
        # Sort by fantasy points PPR and take top performers
        sorted_players = sorted(players, 
                               key=lambda p: p.fantasy_points_ppr or 0, 
                               reverse=True)
        
        return sorted_players[:limit]
    
    @st.cache_data(ttl=600)
    def get_player_season_summary(_self, 
                                 player_id: str, 
                                 season: int) -> Dict[str, Any]:
        """Get season summary statistics for a specific player."""
        player_stats = _self.get_player_stats(season=season, limit=1000)
        player_games = [p for p in player_stats if p.player_id == player_id]
        
        if not player_games:
            return {}
        
        # Calculate season totals and averages
        total_fantasy_points = sum(p.fantasy_points_ppr or 0 for p in player_games)
        total_yards = sum(p.total_yards for p in player_games)
        total_tds = sum(p.total_touchdowns for p in player_games)
        games_played = len(player_games)
        
        return {
            'player_name': player_games[0].player_display_name,
            'position': player_games[0].position,
            'team': player_games[0].team,
            'games_played': games_played,
            'total_fantasy_points': total_fantasy_points,
            'avg_fantasy_points': total_fantasy_points / games_played if games_played > 0 else 0,
            'total_yards': total_yards,
            'total_touchdowns': total_tds,
            'position_group': player_games[0].position_group.value
        }
    
    # Schedule data methods
    @st.cache_data(ttl=3600)  # 1 hour cache for schedule data
    def get_game_schedules(_self,
                          season: Optional[int] = None,
                          week: Optional[int] = None,
                          team: Optional[str] = None,
                          limit: int = 500) -> List[GameSchedule]:
        """Get game schedules with optional filters."""
        
        filters = {}
        if season:
            filters['season'] = season
        if week:
            filters['week'] = week
        
        # For team filter, need to check both home and away
        if team:
            # Custom query to handle team being either home or away
            query = f"""
            SELECT * FROM stg_schedules 
            WHERE (home_team = '{team}' OR away_team = '{team}')
            {f"AND season = {season}" if season else ""}
            {f"AND week = {week}" if week else ""}
            ORDER BY season DESC, week DESC
            LIMIT {limit}
            """
            df = _self.dbt_connector.query_staging_model('stg_schedules', query=query)
        else:
            df = _self.dbt_connector.query_staging_model(
                'stg_schedules',
                filters=filters,
                limit=limit
            )
        
        return dataframe_to_models(df, GameSchedule)
    
    @st.cache_data(ttl=3600)
    def get_games_by_competitiveness(_self, 
                                    competitiveness: GameCompetitiveness,
                                    season: Optional[int] = None) -> List[GameSchedule]:
        """Get games filtered by competitiveness level."""
        games = _self.get_game_schedules(season=season, limit=1000)
        return [g for g in games if g.competitiveness == competitiveness]
    
    @st.cache_data(ttl=3600)
    def get_scoring_statistics(_self, season: Optional[int] = None) -> Dict[str, Any]:
        """Get scoring statistics for games."""
        games = _self.get_game_schedules(season=season, limit=1000)
        completed_games = [g for g in games if g.total_points is not None]
        
        if not completed_games:
            return {}
        
        total_points = [g.total_points for g in completed_games]
        point_differentials = [g.point_differential for g in completed_games]
        
        return {
            'total_games': len(completed_games),
            'avg_total_points': sum(total_points) / len(total_points),
            'max_total_points': max(total_points),
            'min_total_points': min(total_points),
            'avg_point_differential': sum(point_differentials) / len(point_differentials),
            'close_games': len([g for g in completed_games if g.competitiveness in [
                GameCompetitiveness.VERY_CLOSE, GameCompetitiveness.CLOSE
            ]]),
            'blowouts': len([g for g in completed_games if g.competitiveness == GameCompetitiveness.BLOWOUT])
        }
    
    # Play-by-play data methods (limited due to size)
    @st.cache_data(ttl=1800)
    def get_pbp_sample(_self,
                      season: Optional[int] = None,
                      week: Optional[int] = None,
                      game_id: Optional[str] = None,
                      limit: int = 100) -> List[PlayByPlayData]:
        """Get sample play-by-play data with filters."""
        
        filters = {}
        if season:
            filters['season'] = season
        if week:
            filters['week'] = week
        if game_id:
            filters['game_id'] = game_id
        
        df = _self.dbt_connector.query_staging_model(
            'stg_pbp',
            filters=filters,
            limit=limit
        )
        
        return dataframe_to_models(df, PlayByPlayData)
    
    # Data quality and monitoring methods
    def get_data_freshness_summary(self) -> Dict[str, Any]:
        """Get data freshness summary across all staging models."""
        summary = {}
        
        for model_name in self.dbt_connector.staging_models:
            status = self.dbt_connector.get_model_status(model_name)
            dagster_status = self.dagster_monitor.get_dbt_model_dagster_status(model_name)
            
            summary[model_name] = {
                'dbt_status': {
                    'is_fresh': status.is_fresh,
                    'row_count': status.row_count,
                    'last_run': status.last_run.isoformat() if status.last_run else None,
                    'error': status.error
                },
                'dagster_status': {
                    'status': dagster_status.status,
                    'is_fresh': dagster_status.is_fresh,
                    'last_materialization': dagster_status.last_materialization.isoformat() 
                                          if dagster_status.last_materialization else None,
                    'error': dagster_status.error_message
                }
            }
        
        return summary
    
    def trigger_model_refresh(self, model_name: str) -> bool:
        """Trigger refresh of a specific model through Dagster if available."""
        if self.dagster_monitor.is_dagster_available():
            return self.dagster_monitor.trigger_dbt_model_refresh(model_name)
        else:
            # Fallback to direct dbt execution
            result = self.dbt_connector.run_staging_models([model_name])
            return result.success
    
    def get_dashboard_health_status(self) -> Dict[str, Any]:
        """Get overall dashboard health status."""
        freshness_summary = self.get_data_freshness_summary()
        
        total_models = len(freshness_summary)
        fresh_models = sum(1 for m in freshness_summary.values() if m['dbt_status']['is_fresh'])
        error_models = sum(1 for m in freshness_summary.values() if m['dbt_status']['error'])
        
        # Determine overall health
        if error_models > 0:
            health_status = "CRITICAL"
            health_message = f"{error_models} models have errors"
        elif fresh_models < total_models * 0.8:
            health_status = "DEGRADED"
            health_message = f"Only {fresh_models}/{total_models} models are fresh"
        else:
            health_status = "HEALTHY"
            health_message = f"All {total_models} models are operational"
        
        return {
            'status': health_status,
            'message': health_message,
            'total_models': total_models,
            'fresh_models': fresh_models,
            'error_models': error_models,
            'dagster_available': self.dagster_monitor.is_dagster_available(),
            'timestamp': datetime.now().isoformat()
        }


# Singleton service instance for Streamlit
@st.cache_resource
def get_staging_service():
    """Get cached StagingDataService instance."""
    return StagingDataService()


# Convenience functions for common operations
@st.cache_data(ttl=600)
def load_dashboard_data(season: Optional[int] = None) -> Dict[str, Any]:
    """Load all data needed for the main dashboard."""
    service = get_staging_service()
    
    return {
        'teams': service.get_all_teams(),
        'top_players': service.get_top_fantasy_performers(season=season, limit=10),
        'recent_games': service.get_game_schedules(season=season, limit=20),
        'scoring_stats': service.get_scoring_statistics(season=season),
        'health_status': service.get_dashboard_health_status()
    }


@st.cache_data(ttl=300)
def load_team_analysis_data(conference: Optional[Conference] = None) -> Dict[str, Any]:
    """Load data needed for team analysis page."""
    service = get_staging_service()
    
    if conference:
        teams = service.get_teams_by_conference(conference)
    else:
        teams = service.get_all_teams()
    
    return {
        'teams': teams,
        'afc_teams': service.get_teams_by_conference(Conference.AFC),
        'nfc_teams': service.get_teams_by_conference(Conference.NFC)
    }


@st.cache_data(ttl=300)
def load_player_analysis_data(season: Optional[int] = None,
                             position_group: Optional[PositionGroup] = None) -> Dict[str, Any]:
    """Load data needed for player analysis page."""
    service = get_staging_service()
    
    return {
        'top_performers': service.get_top_fantasy_performers(
            season=season, 
            position_group=position_group, 
            limit=50
        ),
        'all_players': service.get_player_stats(
            season=season,
            position_group=position_group,
            limit=200
        )
    }