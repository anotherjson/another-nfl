# NFL Analytics Platform - API Reference

## Overview

This document provides comprehensive API documentation for all components of the NFL Analytics Platform, including CLI commands, Python APIs, WebSocket interfaces, and service endpoints.

## Table of Contents

- [CLI API](#cli-api)
- [Python Library API](#python-library-api)
- [WebSocket API](#websocket-api)
- [Machine Learning API](#machine-learning-api)
- [Real-time Processing API](#real-time-processing-api)
- [Data Extraction API](#data-extraction-api)
- [Configuration API](#configuration-api)

## CLI API

### Data Exploration Commands

#### `explore datasets`
List all available NFL datasets with metadata.

```bash
uv run python -m src.cli explore datasets [OPTIONS]
```

**Options:**
- `--format [table|json|csv]`: Output format (default: table)
- `--verbose`: Show detailed dataset information

**Returns:**
- Table of 19 NFL datasets with start years and descriptions
- Rich formatted output with color coding

#### `explore data`
Sample data from specific NFL dataset.

```bash
uv run python -m src.cli explore data DATASET [OPTIONS]
```

**Parameters:**
- `DATASET`: Dataset name (pbp, weekly, seasonal, etc.)

**Options:**
- `--year INTEGER`: Specific year to explore
- `--limit INTEGER`: Number of rows to display (default: 5)
- `--verbose`: Show detailed error messages

**Returns:**
- Formatted table of sample NFL data
- Column information and data types

#### `read`
Read and analyze parquet files.

```bash
uv run python -m src.cli read FILE_PATH [OPTIONS]
```

**Parameters:**
- `FILE_PATH`: Path to parquet file

**Options:**
- `--info`: Show file metadata and schema information
- `--limit INTEGER`: Number of rows to display

**Returns:**
- File content preview
- Schema information (if --info flag used)

### Data Extraction Commands

#### `extract dataset`
Extract single NFL dataset.

```bash
uv run python -m src.cli extract dataset DATASET [OPTIONS]
```

**Parameters:**
- `DATASET`: Dataset name

**Options:**
- `--year INTEGER`: Year to extract (for year-based datasets)
- `--no-save`: Don't save to disk (memory only)
- `--verbose`: Show detailed progress

**Returns:**
- Extraction summary with row counts and file size
- Progress bar during extraction

#### `extract multiple`
Extract multiple years of a dataset.

```bash
uv run python -m src.cli extract multiple DATASET [OPTIONS]
```

**Parameters:**
- `DATASET`: Dataset name

**Options:**
- `--years TEXT`: Comma-separated years (e.g., "2020,2021,2022")
- `--verbose`: Show detailed progress

**Returns:**
- Summary table of all extractions
- Total time and data volume metrics

#### `extract incremental`
Smart incremental extraction with state tracking.

```bash
uv run python -m src.cli extract incremental DATASET [OPTIONS]
```

**Parameters:**
- `DATASET`: Dataset name

**Options:**
- `--years TEXT`: Specific years to check
- `--max-age-days INTEGER`: Refresh threshold in days (default: 1)
- `--force`: Force refresh regardless of age

**Returns:**
- Status of each year (skipped/extracted/refreshed)
- Incremental processing summary

#### `extract status`
View extraction status and history.

```bash
uv run python -m src.cli extract status [DATASET] [OPTIONS]
```

**Parameters:**
- `DATASET`: Optional specific dataset name

**Options:**
- `--verbose`: Show detailed status information

**Returns:**
- Status table with last extraction times
- Data freshness indicators
- Storage size information

#### `extract cleanup`
Clean up old extraction files.

```bash
uv run python -m src.cli extract cleanup [OPTIONS]
```

**Options:**
- `--max-age-days INTEGER`: Files older than this will be removed
- `--dry-run`: Preview what would be deleted
- `--verbose`: Show detailed cleanup information

**Returns:**
- List of files to be deleted
- Space savings summary

### Machine Learning Commands

#### `analytics train`
Train machine learning models on NFL data.

```bash
uv run python -m src.cli analytics train [OPTIONS]
```

**Options:**
- `--model-type [fantasy|consistency|strength]`: Type of model to train
- `--position [QB|RB|WR|TE|all]`: Position to focus on (default: all)
- `--years TEXT`: Years of data to use for training
- `--save-path TEXT`: Path to save trained model

**Returns:**
- Model training progress and metrics
- Cross-validation scores
- Feature importance rankings

#### `analytics predict`
Generate predictions using trained models.

```bash
uv run python -m src.cli analytics predict [OPTIONS]
```

**Options:**
- `--model-path TEXT`: Path to trained model file
- `--player TEXT`: Specific player name or ID
- `--week INTEGER`: Week to predict for
- `--output [table|json|csv]`: Output format

**Returns:**
- Prediction results with confidence intervals
- Feature contributions to predictions
- Comparative rankings

#### `analytics insights`
Generate analytical insights and reports.

```bash
uv run python -m src.cli analytics insights [OPTIONS]
```

**Options:**
- `--type [consistency|efficiency|breakout|team]`: Analysis type
- `--position [QB|RB|WR|TE]`: Position filter
- `--min-games INTEGER`: Minimum games threshold

**Returns:**
- Rich formatted analytical insights
- Statistical summaries and rankings
- Trend analysis and recommendations

### Real-time Processing Commands

#### `realtime start`
Start real-time processing system.

```bash
uv run python -m src.cli realtime start [OPTIONS]
```

**Options:**
- `--host TEXT`: WebSocket host (default: localhost)
- `--port INTEGER`: WebSocket port (default: 8765)
- `--verbose`: Show detailed processing information

**Returns:**
- WebSocket server status
- Real-time event processing metrics

#### `realtime stream`
Start high-performance stream processing.

```bash
uv run python -m src.cli realtime stream [OPTIONS]
```

**Options:**
- `--duration INTEGER`: Processing duration in seconds
- `--workers INTEGER`: Number of worker threads
- `--window-size INTEGER`: Event window size in minutes

**Returns:**
- Stream processing statistics
- Performance metrics and throughput

#### `realtime dashboard`
Launch real-time Streamlit dashboard.

```bash
uv run python -m src.cli realtime dashboard [OPTIONS]
```

**Options:**
- `--port INTEGER`: Dashboard port (default: 8502)
- `--host TEXT`: Dashboard host

**Returns:**
- Dashboard URL and status
- WebSocket connection information

#### `realtime status`
Check real-time system status.

```bash
uv run python -m src.cli realtime status [OPTIONS]
```

**Options:**
- `--verbose`: Show detailed system information

**Returns:**
- Service status summary
- Active connections and processing metrics
- System health indicators

## Python Library API

### NFLExplorer Class

```python
from src.nfl_explorer import NFLExplorer

explorer = NFLExplorer()
```

#### Methods

##### `list_datasets() -> List[Dict[str, Any]]`
Get list of all available NFL datasets.

**Returns:**
```python
[{
    'name': 'pbp',
    'description': 'Play-by-play data',
    'start_year': 1999,
    'requires_year': True,
    'data_type': 'game_level'
}, ...]
```

##### `get_data(dataset: str, year: Optional[int] = None, limit: int = 5) -> pd.DataFrame`
Fetch sample data from NFL dataset.

**Parameters:**
- `dataset`: Dataset name
- `year`: Optional year filter
- `limit`: Maximum rows to return

**Returns:**
- pandas DataFrame with sample NFL data

**Raises:**
- `ValueError`: Invalid dataset or year
- `APIError`: NFL data API error

### NFLDataExtractor Class

```python
from src.nfl_extractor import NFLDataExtractor

extractor = NFLDataExtractor()
```

#### Methods

##### `extract_dataset(dataset_name: str, year: Optional[int] = None, **kwargs) -> Tuple[pd.DataFrame, Dict]`
Extract NFL dataset with full validation.

**Parameters:**
- `dataset_name`: Name of NFL dataset
- `year`: Year to extract (if applicable)
- `validate`: Enable data validation (default: True)
- `save_to_disk`: Save as parquet file (default: True)

**Returns:**
```python
(
    pd.DataFrame,  # Extracted data
    {
        'dataset': 'pbp',
        'year': 2023,
        'rows': 45287,
        'columns': 372,
        'file_size_mb': 156.8,
        'extraction_time_seconds': 23.4,
        'validation_passed': True
    }
)
```

##### `extract_multiple_years(dataset_name: str, years: List[int], **kwargs) -> List[Dict]`
Extract multiple years of data.

**Parameters:**
- `dataset_name`: Dataset name
- `years`: List of years to extract
- `validate`: Enable validation
- `save_to_disk`: Save files

**Returns:**
- List of extraction result dictionaries

### ExtractionManager Class

```python
from src.extraction_manager import ExtractionManager

manager = ExtractionManager()
```

#### Methods

##### `extract_incremental(dataset_name: str, years: List[int], **kwargs) -> Dict[str, Any]`
Perform incremental extraction with state tracking.

**Parameters:**
- `dataset_name`: Dataset name
- `years`: Years to process
- `force_refresh`: Force re-extraction
- `max_age_days`: Age threshold for refresh

**Returns:**
```python
{
    'dataset': 'weekly',
    'years_processed': [2020, 2021, 2022],
    'years_skipped': [2023],  # Already current
    'years_refreshed': [],
    'total_files': 3,
    'total_rows': 16945,
    'processing_time_seconds': 45.6
}
```

##### `get_extraction_summary(dataset_name: Optional[str] = None) -> Dict[str, Any]`
Get extraction status summary.

**Returns:**
```python
{
    'pbp': {
        'years_available': [2020, 2021, 2022, 2023],
        'last_extraction': '2025-07-22T10:30:00Z',
        'total_rows': 189432,
        'total_size_mb': 623.7,
        'files_count': 4
    },
    # ... other datasets
}
```

### ConfigLoader Class

```python
from src.config_loader import ConfigLoader

loader = ConfigLoader()
```

#### Methods

##### `get_dataset_config(dataset_name: str) -> Dict[str, Any]`
Get configuration for specific dataset.

**Returns:**
```python
{
    'name': 'pbp',
    'description': 'Play-by-play data',
    'function_name': 'load_pbp_data',
    'start_year': 1999,
    'requires_year': True,
    'validation': {
        'required_columns': ['game_id', 'play_id', 'down', 'yards_gained'],
        'expected_size_mb': 150
    },
    'extraction': {
        'timeout_seconds': 300,
        'retry_attempts': 3
    }
}
```

##### `validate_year_for_dataset(dataset: str, year: int) -> bool`
Validate if year is available for dataset.

**Parameters:**
- `dataset`: Dataset name
- `year`: Year to validate

**Returns:**
- `True` if valid, `False` otherwise

**Raises:**
- `ValueError`: Invalid dataset name

## WebSocket API

### Connection

Connect to the real-time WebSocket server:

```python
import websockets
import json

uri = "ws://localhost:8765"
websocket = await websockets.connect(uri)
```

### Message Format

All WebSocket messages follow this format:

```json
{
    "event_type": "score_update",
    "timestamp": "2025-07-22T15:30:45Z",
    "game_id": "game_123",
    "home_team": "Chiefs",
    "away_team": "Bills",
    "data": {
        "old_home_score": 14,
        "old_away_score": 7,
        "new_home_score": 21,
        "new_away_score": 7,
        "scoring_play": "touchdown",
        "player": "Travis Kelce"
    }
}
```

### Event Types

#### Score Updates
```json
{
    "event_type": "score_update",
    "data": {
        "old_home_score": 7,
        "old_away_score": 3,
        "new_home_score": 14,
        "new_away_score": 3,
        "scoring_team": "home",
        "score_type": "touchdown"
    }
}
```

#### Fantasy Updates
```json
{
    "event_type": "fantasy_update",
    "data": {
        "fantasy_players_affected": ["player_123", "player_456"],
        "point_changes": {
            "player_123": 6.5,
            "player_456": 2.1
        },
        "play_description": "15 yard touchdown pass"
    }
}
```

#### Game Events
```json
{
    "event_type": "play_update",
    "data": {
        "play_type": "pass",
        "yards_gained": 15,
        "down": 3,
        "distance": 8,
        "player": "Patrick Mahomes",
        "target": "Travis Kelce"
    }
}
```

### Client Implementation

```python
async def websocket_client():
    uri = "ws://localhost:8765"
    
    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps({
            "type": "subscribe",
            "events": ["score_update", "fantasy_update"]
        }))
        
        async for message in websocket:
            event = json.loads(message)
            await process_event(event)

async def process_event(event):
    event_type = event.get('event_type')
    
    if event_type == 'score_update':
        await handle_score_update(event)
    elif event_type == 'fantasy_update':
        await handle_fantasy_update(event)
```

## Machine Learning API

### NFLAnalytics Class

```python
from src.advanced_analytics import NFLAnalytics

analytics = NFLAnalytics()
```

#### Methods

##### `train_fantasy_model(df: pd.DataFrame, position: str = 'all') -> Dict[str, Any]`
Train fantasy points prediction model.

**Parameters:**
- `df`: NFL data DataFrame
- `position`: Player position filter ('QB', 'RB', 'WR', 'TE', 'all')

**Returns:**
```python
{
    'model_type': 'RandomForestRegressor',
    'position': 'RB',
    'training_samples': 5432,
    'features_used': 15,
    'cv_score': 0.78,
    'mae': 2.3,
    'rmse': 3.1,
    'feature_importance': {
        'rushing_yards': 0.25,
        'targets': 0.18,
        'red_zone_carries': 0.15,
        # ... more features
    }
}
```

##### `predict_fantasy_points(df: pd.DataFrame, position: str = 'all') -> pd.DataFrame`
Generate fantasy points predictions.

**Parameters:**
- `df`: Player data for prediction
- `position`: Position filter

**Returns:**
```python
# DataFrame with columns:
# - player_name
# - predicted_points
# - confidence_lower
# - confidence_upper
# - prediction_rank
```

##### `analyze_player_consistency(df: pd.DataFrame, min_games: int = 8) -> pd.DataFrame`
Analyze player performance consistency.

**Returns:**
```python
# DataFrame with columns:
# - player_name
# - position
# - games_played
# - avg_points
# - std_points
# - consistency_score
# - boom_rate
# - bust_rate
```

##### `analyze_breakout_candidates(df: pd.DataFrame, min_games: int = 8) -> List[Dict]`
Identify breakout candidate players.

**Returns:**
```python
[{
    'player': 'Rookie Johnson',
    'position': 'WR',
    'breakout_score': 0.87,
    'upside_projection': 15.2,
    'risk_factors': ['limited_target_share', 'injury_history'],
    'opportunity_factors': ['target_increase', 'red_zone_usage']
}, ...]
```

### Model Persistence

```python
# Save trained model
model_path = analytics.save_model('fantasy_rb_model.joblib')

# Load trained model
analytics.load_model('fantasy_rb_model.joblib')

# Model metadata
metadata = analytics.get_model_metadata()
```

## Real-time Processing API

### StreamProcessor Class

```python
from src.stream_processor import StreamProcessor, StreamEvent

processor = StreamProcessor(max_workers=4)
```

#### Methods

##### `register_processor(event_type: str, processor_func: Callable) -> None`
Register event processing function.

**Parameters:**
- `event_type`: Type of events to process
- `processor_func`: Function that processes list of events

**Example:**
```python
def process_scores(events: List[StreamEvent]) -> Dict[str, Any]:
    total_points = sum(
        event.data.get('points_scored', 0) 
        for event in events
    )
    return {'total_points': total_points, 'events': len(events)}

processor.register_processor('score_update', process_scores)
```

##### `create_tumbling_window(event_type: str, duration: timedelta) -> None`
Create non-overlapping time windows.

**Parameters:**
- `event_type`: Event type to window
- `duration`: Window duration

##### `create_sliding_window(event_type: str, duration: timedelta, slide: timedelta) -> None`
Create overlapping time windows.

**Parameters:**
- `event_type`: Event type to window
- `duration`: Window duration  
- `slide`: Window slide interval

##### `submit_event(event: StreamEvent) -> None`
Submit event for processing.

**Parameters:**
- `event`: StreamEvent instance

##### `get_stats() -> Dict[str, Any]`
Get processing statistics.

**Returns:**
```python
{
    'events_processed': 15432,
    'windows_processed': 256,
    'errors': 0,
    'runtime_seconds': 1800.5,
    'events_per_second': 8.57,
    'active_windows': 12,
    'watermark': '2025-07-22T15:28:15Z'
}
```

### NFLRealtimeProcessor Class

```python
from src.realtime_processor import NFLRealtimeProcessor

processor = NFLRealtimeProcessor()
```

#### Methods

##### `start_websocket_server(host: str = 'localhost', port: int = 8765) -> None`
Start WebSocket server for real-time updates.

##### `simulate_live_game(game_id: str, duration: int = 180) -> None`
Simulate live game events for testing.

##### `get_active_games() -> List[Dict[str, Any]]`
Get list of currently active games.

##### `get_fantasy_leaderboard() -> List[Dict[str, Any]]`
Get current fantasy points leaderboard.

## Data Extraction API

### Supported Datasets

All 19 NFL datasets from `nfl_data_py`:

| Dataset | Description | Years | Requires Year |
|---------|-------------|--------|---------------|
| `pbp` | Play-by-play data | 1999+ | Yes |
| `weekly` | Weekly player stats | 1999+ | Yes |
| `seasonal` | Seasonal player stats | 1999+ | Yes |
| `schedules` | Game schedules | 1999+ | Yes |
| `team_desc` | Team descriptions | All | No |
| `officials` | Game officials | 2001+ | Yes |
| `combine` | NFL Combine results | 1987+ | Yes |
| `draft_picks` | Draft picks | 1936+ | Yes |
| `qbr` | Weekly QBR data | 2006+ | Yes |
| `injuries` | Injury reports | 2009+ | Yes |
| `players` | Player information | All | No |

### Data Validation

```python
# Validation configuration
{
    'required_columns': ['game_id', 'player_id'],
    'expected_size_mb': 150,
    'row_count_min': 1000,
    'data_types': {
        'game_id': 'string',
        'player_id': 'string',
        'yards_gained': 'numeric'
    }
}
```

### Error Handling

```python
class NFLDataError(Exception):
    """Base exception for NFL data operations"""
    pass

class DatasetNotFoundError(NFLDataError):
    """Dataset not available"""
    pass

class InvalidYearError(NFLDataError):
    """Year not available for dataset"""
    pass

class ValidationError(NFLDataError):
    """Data validation failed"""
    pass

class ExtractionError(NFLDataError):
    """Data extraction failed"""
    pass
```

## Configuration API

### Environment Variables

```bash
# Database configuration
POSTGRES_HOST=localhost
POSTGRES_DB=nfl_platform
POSTGRES_USER=nfl_admin
POSTGRES_PASSWORD=nfl_secure_password

# Data paths
NFL_DATA_PATH=/data/nfl
DBT_TARGET=prod

# Real-time processing
WEBSOCKET_HOST=localhost
WEBSOCKET_PORT=8765
STREAM_WORKERS=4

# Logging
LOG_LEVEL=INFO
LOG_FILE=/var/log/nfl-platform.log
```

### YAML Configuration

Dataset configurations in `configs/datasets/`:

```yaml
# configs/datasets/pbp.yaml
name: pbp
description: "Play-by-play data with detailed game information"
function_name: load_pbp_data
start_year: 1999
requires_year: true
data_type: game_level
partition_by: year

validation:
  required_columns:
    - game_id
    - play_id
    - down
    - yards_gained
  expected_size_mb: 150
  row_count_min: 40000

extraction:
  timeout_seconds: 300
  retry_attempts: 3
  
output:
  file_format: parquet
  compression: snappy
  path_template: "data/{dataset}/{year}/etl_date={etl_date}/data.parquet"
```

## Response Formats

### Success Response
```json
{
    "status": "success",
    "data": { ... },
    "metadata": {
        "timestamp": "2025-07-22T15:30:45Z",
        "processing_time_ms": 234,
        "version": "7.0.0"
    }
}
```

### Error Response
```json
{
    "status": "error",
    "error": {
        "code": "INVALID_DATASET",
        "message": "Dataset 'invalid_name' not found",
        "details": {
            "available_datasets": ["pbp", "weekly", "seasonal"]
        }
    },
    "metadata": {
        "timestamp": "2025-07-22T15:30:45Z",
        "version": "7.0.0"
    }
}
```

## Rate Limits

### NFL Data API
- 100 requests per minute per dataset
- 1000 requests per hour total
- Automatic retry with exponential backoff

### WebSocket Connections
- 100 concurrent connections
- 1000 messages per minute per connection
- Automatic connection management

### Stream Processing
- 10000 events per second processing capacity  
- 1000 concurrent windows
- Memory-based rate limiting

## SDK Examples

### Complete Data Pipeline

```python
from src.nfl_extractor import NFLDataExtractor
from src.advanced_analytics import NFLAnalytics
from src.realtime_processor import NFLRealtimeProcessor

# 1. Extract historical data
extractor = NFLDataExtractor()
data, metadata = extractor.extract_dataset('weekly', year=2023)

# 2. Train ML models
analytics = NFLAnalytics()
model_results = analytics.train_fantasy_model(data, position='RB')

# 3. Start real-time processing
processor = NFLRealtimeProcessor()
processor.start_websocket_server()

# 4. Generate predictions
predictions = analytics.predict_fantasy_points(data)
```

### Custom Integration

```python
import pandas as pd
from src.config_loader import ConfigLoader
from src.extraction_manager import ExtractionManager

# Load configuration
config = ConfigLoader()
pbp_config = config.get_dataset_config('pbp')

# Incremental processing
manager = ExtractionManager()
results = manager.extract_incremental(
    'pbp', 
    years=[2022, 2023], 
    max_age_days=7
)

# Custom analysis
def analyze_team_performance(df):
    return df.groupby('posteam').agg({
        'yards_gained': 'mean',
        'epa': 'mean',
        'success': 'mean'
    })

team_stats = analyze_team_performance(data)
```

---

🚀 **Enterprise API Ready!** The NFL Analytics Platform provides comprehensive APIs for all components with consistent interfaces, robust error handling, and enterprise-grade reliability.