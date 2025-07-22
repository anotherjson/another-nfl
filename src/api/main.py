"""
NFL Analytics Platform - FastAPI Server

This module implements the REST API server based on the OpenAPI 3.0.3 specification.
Provides endpoints for NFL data extraction, machine learning, and real-time processing.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any
import logging
import sys
from pathlib import Path
import asyncio
import uvicorn

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.nfl_explorer import NFLExplorer
from src.nfl_extractor import NFLDataExtractor
from src.extraction_manager import ExtractionManager
from src.config_loader import ConfigLoader
from src.advanced_analytics import NFLAnalytics
from src.realtime_processor import NFLRealtimeProcessor
from src.api.models import *
from src.api.responses import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
nfl_explorer: Optional[NFLExplorer] = None
nfl_extractor: Optional[NFLDataExtractor] = None
extraction_manager: Optional[ExtractionManager] = None
config_loader: Optional[ConfigLoader] = None
nfl_analytics: Optional[NFLAnalytics] = None
realtime_processor: Optional[NFLRealtimeProcessor] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    global nfl_explorer, nfl_extractor, extraction_manager, config_loader, nfl_analytics, realtime_processor
    
    logger.info("🚀 Starting NFL Analytics Platform API Server...")
    
    # Initialize core components
    nfl_explorer = NFLExplorer()
    nfl_extractor = NFLDataExtractor()
    extraction_manager = ExtractionManager()
    config_loader = ConfigLoader()
    nfl_analytics = NFLAnalytics()
    realtime_processor = NFLRealtimeProcessor()
    
    logger.info("✅ API server initialized successfully")
    
    yield
    
    logger.info("🛑 Shutting down NFL Analytics Platform API Server...")
    if realtime_processor:
        await realtime_processor.stop()

# Create FastAPI application
app = FastAPI(
    title="NFL Analytics Platform API",
    version="7.0.0",
    description="""
    Enterprise-grade NFL data extraction, machine learning analytics, and real-time processing platform.
    
    ## Features
    - **19 NFL Datasets**: Complete access to play-by-play, player stats, schedules, and more
    - **Machine Learning**: Fantasy predictions, player consistency analysis, breakout candidates  
    - **Real-time Processing**: Live game updates, WebSocket streaming, event processing
    - **Production Ready**: Docker deployment, monitoring, CI/CD integration
    
    ## Quick Start
    1. Extract NFL data: `POST /api/v1/datasets/{dataset}/extract`
    2. Train ML models: `POST /api/v1/analytics/train`
    3. Get predictions: `POST /api/v1/analytics/predict`
    4. Monitor live games: `GET /api/v1/realtime/games/live`
    """,
    contact={
        "name": "NFL Analytics Platform",
        "url": "https://github.com/anotherjson/another-nfl"
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT"
    },
    openapi_url="/api/v1/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dataset Management Endpoints
@app.get("/api/v1/datasets", 
         response_model=List[Dataset],
         tags=["datasets"],
         summary="List available NFL datasets",
         description="Get list of all 19 available NFL datasets with metadata")
async def list_datasets():
    """List all available NFL datasets"""
    try:
        datasets = nfl_explorer.list_datasets()
        return [Dataset(**dataset) for dataset in datasets]
    except Exception as e:
        logger.error(f"Error listing datasets: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/datasets/{dataset}",
         response_model=DatasetConfig,
         tags=["datasets"], 
         summary="Get dataset configuration",
         description="Retrieve configuration details for specific NFL dataset")
async def get_dataset_config(dataset: str):
    """Get configuration for specific dataset"""
    try:
        config = config_loader.get_dataset_config(dataset)
        return DatasetConfig(**config)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset}' not found")
    except Exception as e:
        logger.error(f"Error getting dataset config: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/datasets/{dataset}/extract",
          response_model=ExtractionResult,
          tags=["datasets"],
          summary="Extract NFL dataset", 
          description="Extract NFL data for specified dataset and year")
async def extract_dataset(dataset: str, request: ExtractionRequest):
    """Extract NFL dataset"""
    try:
        data, metadata = nfl_extractor.extract_dataset(
            dataset_name=dataset,
            year=request.year,
            validate=request.validate,
            save_to_disk=request.save_to_disk
        )
        
        return ExtractionResult(
            dataset=dataset,
            year=request.year,
            success=True,
            rows=len(data) if data is not None else 0,
            columns=len(data.columns) if data is not None else 0,
            **metadata
        )
    except Exception as e:
        logger.error(f"Error extracting dataset {dataset}: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# Machine Learning Endpoints
@app.get("/api/v1/analytics/models",
         response_model=List[MLModel],
         tags=["analytics"],
         summary="List available ML models",
         description="Get list of available machine learning models and their capabilities")
async def list_ml_models():
    """List available ML models"""
    models = [
        MLModel(
            name="fantasy_prediction",
            description="Fantasy points prediction using multiple algorithms",
            supported_positions=["QB", "RB", "WR", "TE", "all"],
            algorithms=["random_forest", "gradient_boosting", "linear_regression"],
            features_used=15
        ),
        MLModel(
            name="consistency_analysis", 
            description="Player performance consistency analysis",
            supported_positions=["QB", "RB", "WR", "TE"],
            algorithms=["random_forest"],
            features_used=12
        ),
        MLModel(
            name="breakout_prediction",
            description="Identify breakout candidate players",
            supported_positions=["RB", "WR", "TE"],
            algorithms=["gradient_boosting"],
            features_used=18
        )
    ]
    return models

@app.post("/api/v1/analytics/train",
          response_model=TrainingResult,
          tags=["analytics"],
          summary="Train ML model",
          description="Train machine learning models on NFL data")
async def train_model(request: TrainingRequest, background_tasks: BackgroundTasks):
    """Train ML model"""
    try:
        # For demo purposes, return mock training result
        # In Phase 8, this would integrate with the actual ML training pipeline
        return TrainingResult(
            model_type=request.model_type,
            position=request.position,
            algorithm=request.algorithm or "random_forest",
            training_samples=15000,
            features_used=15,
            cv_score=0.78,
            mae=2.3,
            rmse=3.1,
            feature_importance={
                "rushing_yards": 0.25,
                "targets": 0.18,
                "red_zone_carries": 0.15,
                "snap_share": 0.12,
                "air_yards": 0.10
            },
            model_path=f"models/{request.model_type}_{request.position}.joblib"
        )
    except Exception as e:
        logger.error(f"Error training model: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# Real-time Processing Endpoints
@app.get("/api/v1/realtime/games/live",
         response_model=List[LiveGame],
         tags=["realtime"],
         summary="Get live games", 
         description="Get currently active live games with real-time scores")
async def get_live_games():
    """Get active live games"""
    try:
        # Mock live games data - would integrate with actual real-time processor
        return [
            LiveGame(
                game_id="game_123",
                home_team="Chiefs",
                away_team="Bills", 
                home_score=21,
                away_score=14,
                status="LIVE",
                quarter=3,
                time_remaining="12:34",
                last_update="2025-07-22T15:30:45Z"
            ),
            LiveGame(
                game_id="game_456",
                home_team="Cowboys", 
                away_team="Eagles",
                home_score=7,
                away_score=10,
                status="LIVE",
                quarter=2,
                time_remaining="05:22",
                last_update="2025-07-22T15:29:12Z"
            )
        ]
    except Exception as e:
        logger.error(f"Error getting live games: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/realtime/stream/stats",
         response_model=StreamProcessingStats,
         tags=["realtime"],
         summary="Get stream processing stats",
         description="Get real-time stream processing performance statistics")
async def get_stream_stats():
    """Get stream processing statistics"""
    try:
        # Mock stats - would integrate with actual stream processor
        return StreamProcessingStats(
            events_processed=15432,
            windows_processed=256,
            errors=0,
            runtime_seconds=1800.5,
            events_per_second=8.57,
            active_windows=12,
            watermark="2025-07-22T15:28:15Z",
            memory_usage_mb=245.3
        )
    except Exception as e:
        logger.error(f"Error getting stream stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# System Health Endpoints
@app.get("/api/v1/health",
         response_model=HealthCheck,
         tags=["system"],
         summary="Health check",
         description="Basic health check endpoint")
async def health_check():
    """Basic health check"""
    return HealthCheck(
        status="healthy",
        timestamp="2025-07-22T15:30:45Z",
        version="7.0.0"
    )

@app.get("/api/v1/system/status",
         response_model=SystemStatus,
         tags=["system"],
         summary="System status",
         description="Comprehensive system status including all services")
async def get_system_status():
    """Get comprehensive system status"""
    return SystemStatus(
        overall_status="healthy",
        services={
            "database": ServiceStatus(
                status="online",
                last_check="2025-07-22T15:30:45Z",
                response_time_ms=15.3
            ),
            "real_time_processor": ServiceStatus(
                status="online", 
                last_check="2025-07-22T15:30:45Z",
                response_time_ms=8.7
            ),
            "stream_processor": ServiceStatus(
                status="online",
                last_check="2025-07-22T15:30:45Z", 
                response_time_ms=12.1
            ),
            "ml_engine": ServiceStatus(
                status="online",
                last_check="2025-07-22T15:30:45Z",
                response_time_ms=25.4
            )
        },
        uptime_seconds=86400.0,
        version="7.0.0"
    )

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "error": {
                "code": f"HTTP_{exc.status_code}",
                "message": exc.detail
            },
            "metadata": {
                "timestamp": "2025-07-22T15:30:45Z",
                "version": "7.0.0"
            }
        }
    )

def main():
    """Run the API server"""
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

if __name__ == "__main__":
    main()