"""
Pydantic models for the NFL Analytics Platform API

These models define the request/response schemas used by the FastAPI application
and correspond to the OpenAPI specification.
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum

# Enums
class Position(str, Enum):
    QB = "QB"
    RB = "RB" 
    WR = "WR"
    TE = "TE"
    K = "K"
    DEF = "DEF"
    ALL = "all"

class EventType(str, Enum):
    SCORE_UPDATE = "score_update"
    FANTASY_UPDATE = "fantasy_update"
    PLAY_UPDATE = "play_update"
    GAME_START = "game_start"
    GAME_END = "game_end"
    INJURY_UPDATE = "injury_update"
    TIMEOUT_CALLED = "timeout_called"
    PENALTY_CALLED = "penalty_called"

class GameStatus(str, Enum):
    LIVE = "LIVE"
    FINAL = "FINAL"
    SCHEDULED = "SCHEDULED"
    HALFTIME = "HALFTIME"

class ServiceStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline" 
    DEGRADED = "degraded"

# Core Data Models
class Dataset(BaseModel):
    name: str = Field(..., description="Dataset identifier", example="pbp")
    description: str = Field(..., description="Human-readable description", example="Play-by-play data with detailed game information")
    start_year: Optional[int] = Field(None, description="Earliest available year", example=1999)
    requires_year: bool = Field(..., description="Whether this dataset requires a year parameter", example=True)
    data_type: str = Field(..., description="Category of data", example="game_level")
    estimated_size_mb: Optional[float] = Field(None, description="Estimated file size in MB", example=150.5)

class DatasetConfig(Dataset):
    validation: Optional[Dict[str, Any]] = None
    extraction: Optional[Dict[str, Any]] = None
    output: Optional[Dict[str, Any]] = None

# Extraction Models
class ExtractionRequest(BaseModel):
    year: Optional[int] = Field(None, ge=1936, le=2030, description="Year to extract", example=2023)
    validate: bool = Field(True, description="Enable data validation")
    save_to_disk: bool = Field(True, description="Save extracted data to parquet file")
    verbose: bool = Field(False, description="Enable verbose logging")

class ExtractionResult(BaseModel):
    dataset: str = Field(..., example="pbp")
    year: Optional[int] = Field(None, example=2023)
    success: bool = Field(..., example=True)
    rows: int = Field(..., description="Number of rows extracted", example=45287)
    columns: int = Field(..., description="Number of columns", example=372)
    file_size_mb: Optional[float] = Field(None, description="File size in MB", example=156.8)
    extraction_time_seconds: float = Field(..., description="Time taken for extraction", example=23.4)
    validation_passed: bool = Field(..., description="Whether validation checks passed", example=True)
    file_path: Optional[str] = Field(None, description="Path to saved file", example="data/pbp/2023/etl_date=2025-07-22/data.parquet")

class MultiYearExtractionRequest(BaseModel):
    years: List[int] = Field(..., min_items=1, example=[2020, 2021, 2022, 2023])
    validate: bool = Field(True)
    save_to_disk: bool = Field(True)
    verbose: bool = Field(False)

class MultiYearExtractionResult(BaseModel):
    dataset: str
    years_processed: List[int]
    results: List[ExtractionResult]
    total_rows: int
    total_size_mb: float
    total_time_seconds: float

class IncrementalExtractionRequest(BaseModel):
    years: Optional[List[int]] = Field(None, description="Specific years to check")
    max_age_days: int = Field(1, ge=0, description="Refresh threshold in days")
    force_refresh: bool = Field(False, description="Force refresh regardless of age")

class IncrementalExtractionResult(BaseModel):
    dataset: str
    years_processed: List[int] = Field(..., description="Years that were extracted")
    years_skipped: List[int] = Field(..., description="Years that were skipped")
    years_refreshed: List[int] = Field(..., description="Years that were refreshed")
    processing_time_seconds: float

class ExtractionStatus(BaseModel):
    dataset: str
    years_available: List[int]
    last_extraction: datetime
    total_rows: int
    total_size_mb: float
    files_count: int

# Machine Learning Models
class MLModel(BaseModel):
    name: str = Field(..., example="fantasy_prediction")
    description: str
    supported_positions: List[Position]
    algorithms: List[str]
    features_used: int = Field(..., example=15)

class TrainingRequest(BaseModel):
    model_type: str = Field(..., description="Type of model to train", example="fantasy")
    position: Position = Field(Position.ALL, description="Player position")
    years: List[int] = Field(..., example=[2020, 2021, 2022, 2023])
    algorithm: Optional[str] = Field("auto", description="ML algorithm to use")
    save_model: bool = Field(True, description="Whether to save trained model")
    model_name: Optional[str] = Field(None, description="Custom name for saved model")

class TrainingResult(BaseModel):
    model_type: str
    position: Position
    algorithm: str
    training_samples: int
    features_used: int
    cv_score: float = Field(..., description="Cross-validation score")
    mae: float = Field(..., description="Mean Absolute Error")
    rmse: float = Field(..., description="Root Mean Square Error")
    feature_importance: Dict[str, float]
    model_path: Optional[str] = Field(None, description="Path to saved model file")

class PredictionRequest(BaseModel):
    model_type: str = Field(..., description="Type of model for prediction")
    model_path: Optional[str] = Field(None, description="Path to specific model file")
    position: Position = Field(Position.ALL)
    player_name: Optional[str] = Field(None, description="Specific player to predict for")
    week: Optional[int] = Field(None, ge=1, le=18, description="Week to predict for")
    data: Optional[Dict[str, Any]] = Field(None, description="Custom player data for prediction")

class PlayerPrediction(BaseModel):
    player_name: str
    predicted_points: float
    confidence_lower: float
    confidence_upper: float
    prediction_rank: int

class PredictionResult(BaseModel):
    model_type: str
    position: Position
    predictions: List[PlayerPrediction]

class AnalyticalInsight(BaseModel):
    category: str
    description: str
    players: List[str]
    statistics: Dict[str, float]

class AnalyticalInsights(BaseModel):
    insight_type: str
    position: Position
    insights: List[AnalyticalInsight]

# Real-time Processing Models
class LiveGame(BaseModel):
    game_id: str = Field(..., example="game_123")
    home_team: str = Field(..., example="Chiefs")
    away_team: str = Field(..., example="Bills")
    home_score: int = Field(..., example=21)
    away_score: int = Field(..., example=14)
    status: GameStatus = Field(..., example="LIVE")
    quarter: Optional[int] = Field(None, ge=1, le=5)
    time_remaining: Optional[str] = Field(None, example="12:34")
    last_update: str = Field(..., example="2025-07-22T15:30:45Z")

class StreamEvent(BaseModel):
    event_type: EventType
    timestamp: str = Field(..., example="2025-07-22T15:30:45Z")
    game_id: str = Field(..., example="game_123")
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    data: Dict[str, Any] = Field(..., description="Event-specific data payload")

class EventSubmissionResult(BaseModel):
    accepted: bool
    event_id: str
    message: str

class FantasyPlayer(BaseModel):
    rank: int
    player_name: str
    team: str
    position: Position
    fantasy_points: float
    game_status: str

class FantasyLeaderboard(BaseModel):
    position: Position
    last_updated: str
    players: List[FantasyPlayer]

class StreamProcessingStats(BaseModel):
    events_processed: int = Field(..., example=15432)
    windows_processed: int = Field(..., example=256)
    errors: int = Field(..., example=0)
    runtime_seconds: float = Field(..., example=1800.5)
    events_per_second: float = Field(..., example=8.57)
    active_windows: int = Field(..., example=12)
    watermark: str = Field(..., example="2025-07-22T15:28:15Z")
    memory_usage_mb: float = Field(..., example=245.3)

# System Models
class HealthCheck(BaseModel):
    status: str = Field(..., example="healthy")
    timestamp: str = Field(..., example="2025-07-22T15:30:45Z")
    version: str = Field(..., example="7.0.0")

class ServiceStatusInfo(BaseModel):
    status: ServiceStatus
    last_check: str
    response_time_ms: float
    error_message: Optional[str] = None

class SystemStatus(BaseModel):
    overall_status: str
    services: Dict[str, ServiceStatusInfo]
    uptime_seconds: float
    version: str

class SystemMetrics(BaseModel):
    cpu_usage_percent: float
    memory_usage_percent: float
    disk_usage_percent: float
    active_connections: int
    requests_per_minute: float
    error_rate_percent: float

class AsyncTaskStatus(BaseModel):
    task_id: str
    status: str = Field(..., description="Task status")
    progress_percent: float = Field(..., ge=0, le=100)
    estimated_completion: Optional[str] = Field(None, description="Estimated completion time")

# Error Models
class ErrorDetail(BaseModel):
    code: str = Field(..., example="INVALID_DATASET")
    message: str = Field(..., example="Dataset 'invalid_name' not found")
    details: Optional[Dict[str, Any]] = None

class ErrorMetadata(BaseModel):
    timestamp: str
    version: str

class ErrorResponse(BaseModel):
    status: str = Field("error")
    error: ErrorDetail
    metadata: ErrorMetadata

class ValidationErrorField(BaseModel):
    field: str
    message: str
    value: Optional[Any] = None

class ValidationErrorResponse(ErrorResponse):
    validation_errors: List[ValidationErrorField]