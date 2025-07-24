"""
Data models and type definitions for NFL Analytics Streamlit Dashboard.

Provides structured data models that map to dbt staging models with type safety
and validation for consistent data handling across the application.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from enum import Enum
import pandas as pd


class PositionGroup(Enum):
    """NFL position groups for categorization."""
    QUARTERBACK = "Quarterback"
    RUNNING_BACK = "Running Back" 
    WIDE_RECEIVER = "Wide Receiver"
    TIGHT_END = "Tight End"
    KICKER = "Kicker"
    DEFENSE = "Defense"
    OTHER = "Other"


class Conference(Enum):
    """NFL conferences."""
    AFC = "AFC"
    NFC = "NFC"


class FantasyTier(Enum):
    """Fantasy performance tiers."""
    ELITE = "Elite (20+)"
    GREAT = "Great (15-19.9)"
    GOOD = "Good (10-14.9)"
    OKAY = "Okay (5-9.9)"
    POOR = "Poor (0.1-4.9)"
    ZERO = "Zero (0)"


class GameCompetitiveness(Enum):
    """Game competitiveness categories."""
    VERY_CLOSE = "Very Close (0-3)"
    CLOSE = "Close (4-7)"
    MODERATE = "Moderate (8-14)"
    BLOWOUT = "Blowout (15+)"


@dataclass
class TeamInfo:
    """Team information from stg_team_desc model."""
    team_abbr: str
    team_id: Optional[str]
    team_name: str
    team_nick: Optional[str]
    team_color: Optional[str]
    team_color2: Optional[str]
    team_logo_espn: Optional[str]
    team_logo_wikipedia: Optional[str]
    team_conference: Conference
    team_division: str
    dbt_loaded_at: datetime
    dbt_run_id: str
    
    @classmethod
    def from_dataframe_row(cls, row: pd.Series) -> 'TeamInfo':
        """Create TeamInfo from a DataFrame row."""
        return cls(
            team_abbr=row['team_abbr'],
            team_id=row.get('team_id'),
            team_name=row['team_name'],
            team_nick=row.get('team_nick'),
            team_color=row.get('team_color'),
            team_color2=row.get('team_color2'),
            team_logo_espn=row.get('team_logo_espn'),
            team_logo_wikipedia=row.get('team_logo_wikipedia'),
            team_conference=Conference(row['team_conference']),
            team_division=row['team_division'],
            dbt_loaded_at=pd.to_datetime(row['dbt_loaded_at']),
            dbt_run_id=row['dbt_run_id']
        )


@dataclass
class PlayerWeeklyStats:
    """Player weekly statistics from stg_weekly model."""
    player_id: str
    player_name: str
    player_display_name: str
    position: str
    team: str
    opponent_team: Optional[str]
    season: int
    week: int
    
    # Passing stats
    completions: Optional[int]
    attempts: Optional[int]
    passing_yards: Optional[int]
    passing_tds: Optional[int]
    interceptions: Optional[int]
    
    # Rushing stats
    carries: Optional[int]
    rushing_yards: Optional[int]
    rushing_tds: Optional[int]
    
    # Receiving stats
    targets: Optional[int]
    receptions: Optional[int]
    receiving_yards: Optional[int]
    receiving_tds: Optional[int]
    
    # Fantasy points
    fantasy_points: Optional[float]
    fantasy_points_ppr: Optional[float]
    
    # Metadata
    dbt_loaded_at: datetime
    dbt_run_id: str
    
    @property
    def position_group(self) -> PositionGroup:
        """Get position group classification."""
        if self.position == 'QB':
            return PositionGroup.QUARTERBACK
        elif self.position in ['RB', 'FB']:
            return PositionGroup.RUNNING_BACK
        elif self.position == 'WR':
            return PositionGroup.WIDE_RECEIVER
        elif self.position == 'TE':
            return PositionGroup.TIGHT_END
        elif self.position == 'K':
            return PositionGroup.KICKER
        elif self.position == 'DEF':
            return PositionGroup.DEFENSE
        else:
            return PositionGroup.OTHER
    
    @property
    def total_yards(self) -> int:
        """Calculate total yards from all sources."""
        return sum(filter(None, [
            self.passing_yards or 0,
            self.rushing_yards or 0,
            self.receiving_yards or 0
        ]))
    
    @property
    def total_touchdowns(self) -> int:
        """Calculate total touchdowns from all sources."""
        return sum(filter(None, [
            self.passing_tds or 0,
            self.rushing_tds or 0,
            self.receiving_tds or 0
        ]))
    
    @property
    def fantasy_tier(self) -> FantasyTier:
        """Get fantasy performance tier."""
        points = self.fantasy_points_ppr or 0
        
        if points >= 20:
            return FantasyTier.ELITE
        elif points >= 15:
            return FantasyTier.GREAT
        elif points >= 10:
            return FantasyTier.GOOD
        elif points >= 5:
            return FantasyTier.OKAY
        elif points > 0:
            return FantasyTier.POOR
        else:
            return FantasyTier.ZERO
    
    @classmethod
    def from_dataframe_row(cls, row: pd.Series) -> 'PlayerWeeklyStats':
        """Create PlayerWeeklyStats from a DataFrame row."""
        return cls(
            player_id=row['player_id'],
            player_name=row['player_name'],
            player_display_name=row['player_display_name'],
            position=row['position'],
            team=row['team'],
            opponent_team=row.get('opponent_team'),
            season=int(row['season']),
            week=int(row['week']),
            completions=row.get('completions'),
            attempts=row.get('attempts'),
            passing_yards=row.get('passing_yards'),
            passing_tds=row.get('passing_tds'),
            interceptions=row.get('interceptions'),
            carries=row.get('carries'),
            rushing_yards=row.get('rushing_yards'),
            rushing_tds=row.get('rushing_tds'),
            targets=row.get('targets'),
            receptions=row.get('receptions'),
            receiving_yards=row.get('receiving_yards'),
            receiving_tds=row.get('receiving_tds'),
            fantasy_points=row.get('fantasy_points'),
            fantasy_points_ppr=row.get('fantasy_points_ppr'),
            dbt_loaded_at=pd.to_datetime(row['dbt_loaded_at']),
            dbt_run_id=row['dbt_run_id']
        )


@dataclass
class GameSchedule:
    """Game schedule information from stg_schedules model."""
    game_id: str
    season: int
    week: int
    home_team: str
    away_team: str
    home_score: Optional[int]
    away_score: Optional[int]
    dbt_loaded_at: datetime
    dbt_run_id: str
    
    @property
    def winner(self) -> Optional[str]:
        """Determine winning team."""
        if self.home_score is None or self.away_score is None:
            return None
        
        if self.home_score > self.away_score:
            return self.home_team
        elif self.away_score > self.home_score:
            return self.away_team
        else:
            return "TIE"
    
    @property
    def total_points(self) -> Optional[int]:
        """Calculate total points scored."""
        if self.home_score is None or self.away_score is None:
            return None
        return self.home_score + self.away_score
    
    @property
    def point_differential(self) -> Optional[int]:
        """Calculate point differential."""
        if self.home_score is None or self.away_score is None:
            return None
        return abs(self.home_score - self.away_score)
    
    @property
    def competitiveness(self) -> Optional[GameCompetitiveness]:
        """Get game competitiveness category."""
        diff = self.point_differential
        if diff is None:
            return None
        
        if diff <= 3:
            return GameCompetitiveness.VERY_CLOSE
        elif diff <= 7:
            return GameCompetitiveness.CLOSE
        elif diff <= 14:
            return GameCompetitiveness.MODERATE
        else:
            return GameCompetitiveness.BLOWOUT
    
    @classmethod
    def from_dataframe_row(cls, row: pd.Series) -> 'GameSchedule':
        """Create GameSchedule from a DataFrame row."""
        return cls(
            game_id=row['game_id'],
            season=int(row['season']),
            week=int(row['week']),
            home_team=row['home_team'],
            away_team=row['away_team'],
            home_score=row.get('home_score'),
            away_score=row.get('away_score'),
            dbt_loaded_at=pd.to_datetime(row['dbt_loaded_at']),
            dbt_run_id=row['dbt_run_id']
        )


@dataclass
class PlayByPlayData:
    """Play-by-play data from stg_pbp model (subset of fields for dashboard)."""
    game_id: str
    play_id: int
    season: int
    week: int
    game_date: datetime
    home_team: str
    away_team: str
    possession_team: Optional[str]
    defense_team: Optional[str]
    quarter: Optional[int]
    down: Optional[int]
    yards_to_go: Optional[int]
    yardline_100: Optional[int]
    play_type: Optional[str]
    play_description: Optional[str]
    touchdown: Optional[bool]
    field_goal_attempt: Optional[bool]
    epa: Optional[float]
    wp: Optional[float]
    dbt_loaded_at: datetime
    dbt_run_id: str
    
    @classmethod
    def from_dataframe_row(cls, row: pd.Series) -> 'PlayByPlayData':
        """Create PlayByPlayData from a DataFrame row."""
        return cls(
            game_id=row['game_id'],
            play_id=int(row['play_id']),
            season=int(row['season']),
            week=int(row['week']),
            game_date=pd.to_datetime(row['game_date']),
            home_team=row['home_team'],
            away_team=row['away_team'],
            possession_team=row.get('possession_team'),
            defense_team=row.get('defense_team'),
            quarter=row.get('quarter'),
            down=row.get('down'),
            yards_to_go=row.get('yards_to_go'),
            yardline_100=row.get('yardline_100'),
            play_type=row.get('play_type'),
            play_description=row.get('play_description'),
            touchdown=row.get('touchdown'),
            field_goal_attempt=row.get('field_goal_attempt'),
            epa=row.get('epa'),
            wp=row.get('wp'),
            dbt_loaded_at=pd.to_datetime(row['dbt_loaded_at']),
            dbt_run_id=row['dbt_run_id']
        )


# Utility functions for data model operations
def dataframe_to_models(df: pd.DataFrame, model_class) -> List[Any]:
    """Convert a DataFrame to a list of data model instances."""
    models = []
    for _, row in df.iterrows():
        try:
            model = model_class.from_dataframe_row(row)
            models.append(model)
        except Exception as e:
            # Skip rows that can't be converted (log error in production)
            continue
    return models


def models_to_dataframe(models: List[Any]) -> pd.DataFrame:
    """Convert a list of data model instances back to a DataFrame."""
    if not models:
        return pd.DataFrame()
    
    # Convert each model to dict and create DataFrame
    data = [model.__dict__ for model in models]
    return pd.DataFrame(data)


def filter_models(models: List[Any], **filters) -> List[Any]:
    """Filter a list of models by attribute values."""
    filtered = []
    
    for model in models:
        match = True
        for attr, value in filters.items():
            if not hasattr(model, attr):
                match = False
                break
            
            model_value = getattr(model, attr)
            
            # Handle enum comparisons
            if hasattr(model_value, 'value'):
                model_value = model_value.value
            
            if model_value != value:
                match = False
                break
        
        if match:
            filtered.append(model)
    
    return filtered