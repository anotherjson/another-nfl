# NFL Real-time Data Processing Guide

## Overview

The NFL Analytics Platform now includes comprehensive real-time data processing capabilities that enable live game tracking, fantasy analytics, and event-driven insights. This Phase 7 implementation provides WebSocket integration, stream processing, and live dashboards for enterprise-grade real-time analytics.

## Architecture

### Real-time Processing Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │  Stream Engine  │    │  Live Dashboards│
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • NFL APIs      │───▶│ • Event Buffer  │───▶│ • Streamlit     │
│ • Live Feeds    │    │ • Window Proc.  │    │ • WebSocket UI  │
│ • Game Updates  │    │ • ML Inference  │    │ • Fantasy Track │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               │
                               ▼
                       ┌─────────────────┐
                       │  WebSocket Hub  │
                       ├─────────────────┤
                       │ • Event Routing │
                       │ • Client Mgmt   │
                       │ • Live Updates  │
                       └─────────────────┘
```

### Core Services

1. **Real-time Processor** (`src/realtime_processor.py`)
   - WebSocket server (port 8765)
   - Event processing and distribution
   - Fantasy live tracking
   - Multi-client support

2. **Stream Processing Engine** (`src/stream_processor.py`)
   - High-throughput event processing
   - Window-based aggregations
   - Complex event pattern detection
   - Multi-threaded performance

3. **Live Dashboard** (`visualizations/streamlit_app/realtime_dashboard.py`)
   - Real-time game updates
   - Live fantasy tracking
   - Event feed monitoring
   - WebSocket client integration

## Quick Start

### 1. Start Real-time Services

```bash
# Start real-time processor (WebSocket server)
uv run python -m src.realtime_processor

# Start real-time dashboard (separate terminal)
uv run streamlit run visualizations/streamlit_app/realtime_dashboard.py --server.port 8502
```

### 2. Docker Deployment

```bash
# Start all services including real-time components
docker-compose up -d

# Access services:
# - Real-time Dashboard: http://localhost:8502
# - WebSocket Server: ws://localhost:8765
# - Standard Dashboard: http://localhost:8501
```

### 3. CLI Real-time Commands

```bash
# Start real-time processor
uv run python -m src.cli realtime start

# Start stream processor for high-throughput processing
uv run python -m src.cli realtime stream --duration 300

# Launch real-time dashboard
uv run python -m src.cli realtime dashboard

# Check system status
uv run python -m src.cli realtime status
```

## WebSocket Integration

### Server Implementation

The WebSocket server provides real-time event distribution:

```python
# Example WebSocket message format
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
        "scoring_play": "touchdown"
    }
}
```

### Client Connection

Connect to the WebSocket server:

```python
import websockets
import json
import asyncio

async def listen_for_updates():
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as websocket:
        async for message in websocket:
            event = json.loads(message)
            print(f"Received: {event['event_type']}")
```

## Stream Processing

### Window Types

The stream processor supports multiple window types:

#### Tumbling Windows
Non-overlapping fixed-duration windows:
```python
# 1-minute tumbling windows for real-time analysis
processor.create_tumbling_window('score_update', timedelta(minutes=1))
```

#### Sliding Windows
Overlapping windows for trend analysis:
```python
# 5-minute windows sliding every 1 minute
processor.create_sliding_window('score_update', 
                               duration=timedelta(minutes=5), 
                               slide=timedelta(minutes=1))
```

#### Session Windows
Activity-based variable windows:
```python
# Automatic session detection based on event patterns
processor.create_session_window('game_events', idle_timeout=timedelta(minutes=10))
```

### Event Processing

#### Score Updates
```python
def process_score_updates(events: List[StreamEvent]) -> Dict[str, Any]:
    results = {
        'total_events': len(events),
        'games_affected': set(),
        'total_points_scored': 0,
        'scoring_plays': []
    }
    
    for event in events:
        # Process scoring logic
        points_scored = calculate_points_from_event(event)
        results['total_points_scored'] += points_scored
    
    return results
```

#### Fantasy Updates
```python
def process_fantasy_updates(events: List[StreamEvent]) -> Dict[str, Any]:
    player_points = defaultdict(float)
    
    for event in events:
        players = event.data.get('fantasy_players_affected', [])
        point_changes = event.data.get('point_changes', {})
        
        for player_id in players:
            player_points[player_id] += point_changes.get(player_id, 0)
    
    return {
        'players_affected': len(player_points),
        'total_fantasy_points': sum(player_points.values()),
        'top_performers': sorted(player_points.items(), 
                               key=lambda x: x[1], reverse=True)[:10]
    }
```

## Live Dashboard Features

### Game Tracking
- Real-time score updates
- Live game status
- Multi-game monitoring
- Last update timestamps

### Fantasy Analytics
- Live point calculations
- Player performance tracking
- Real-time rankings
- Point change history

### Event Feed
- Chronological event display
- Event type filtering
- Expandable event details
- Auto-refresh capabilities

### Connection Management
- WebSocket status monitoring
- Manual connect/disconnect
- Auto-reconnection handling
- Error state management

## Performance Optimization

### Stream Processing
```python
# Multi-threaded processing configuration
processor = StreamProcessor(max_workers=4)

# Batch processing for efficiency
batch_size = 100
events_processed_per_second = 1000+

# Memory management
event_queue_max_size = 10000
window_retention_time = timedelta(hours=1)
```

### WebSocket Performance
```python
# Connection management
max_concurrent_clients = 100
message_queue_size = 1000
heartbeat_interval = 30  # seconds

# Message compression
compression_enabled = True
max_message_size = 64 * 1024  # 64KB
```

## Monitoring & Metrics

### Processing Statistics
```python
stats = processor.get_stats()
# Returns:
# {
#     'events_processed': 15432,
#     'windows_processed': 256,
#     'errors': 0,
#     'runtime_seconds': 1800.5,
#     'events_per_second': 8.57,
#     'active_windows': 12,
#     'watermark': '2025-07-22T15:28:15Z'
# }
```

### Health Checks
```bash
# Check real-time processor status
curl -f http://localhost:8765/health

# Stream processor metrics
python -c "from src.stream_processor import StreamProcessor; 
           print(StreamProcessor().get_stats())"

# Dashboard health
curl -f http://localhost:8502/_stcore/health
```

## Event Types

### Core Events
- `score_update`: Game score changes
- `game_start`: Game initiation
- `game_end`: Game completion
- `play_update`: Individual play events
- `fantasy_update`: Fantasy point changes

### Extended Events  
- `injury_update`: Player injury reports
- `timeout_called`: Team timeouts
- `penalty_called`: Game penalties
- `weather_update`: Game condition changes
- `broadcast_update`: Coverage information

## Integration Examples

### Custom Event Processor
```python
from src.stream_processor import StreamProcessor
from src.realtime_processor import StreamEvent

# Create custom processor
def custom_touchdown_processor(events: List[StreamEvent]) -> Dict:
    touchdowns = []
    for event in events:
        if event.data.get('play_type') == 'touchdown':
            touchdowns.append({
                'player': event.data.get('player'),
                'team': event.data.get('team'),
                'yards': event.data.get('yards'),
                'timestamp': event.timestamp.isoformat()
            })
    
    return {'touchdowns': touchdowns, 'count': len(touchdowns)}

# Register processor
processor = StreamProcessor()
processor.register_processor('play_update', custom_touchdown_processor)
```

### WebSocket Client Integration
```python
import streamlit as st
import websockets
import json
import asyncio

@st.cache_resource
def get_websocket_connection():
    return WebSocketConnection("ws://localhost:8765")

class WebSocketConnection:
    def __init__(self, uri):
        self.uri = uri
        self.connection = None
    
    async def connect(self):
        self.connection = await websockets.connect(self.uri)
    
    async def listen(self, callback):
        async for message in self.connection:
            data = json.loads(message)
            callback(data)
```

## Troubleshooting

### Common Issues

#### WebSocket Connection Failed
```bash
# Check if processor is running
ps aux | grep realtime_processor

# Verify port availability
netstat -ln | grep :8765

# Check firewall settings
sudo ufw status
```

#### Stream Processing Lag
```bash
# Monitor queue sizes
python -c "from src.stream_processor import StreamProcessor; 
           s = StreamProcessor(); print(f'Queue size: {len(s.event_queue)}')"

# Check memory usage
htop -p $(pgrep -f realtime_processor)

# Increase workers
processor = StreamProcessor(max_workers=8)
```

#### Dashboard Not Updating
```bash
# Check WebSocket connection in browser dev tools
# Network tab -> WS -> Messages

# Verify Streamlit auto-refresh
# Should see periodic refreshes in console

# Check session state
# Use debug expander in dashboard
```

### Debug Mode
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
uv run python -m src.realtime_processor

# Verbose stream processing
uv run python -m src.cli realtime stream --verbose --debug

# Dashboard debug mode
streamlit run visualizations/streamlit_app/realtime_dashboard.py --server.runOnSave true --logger.level debug
```

## Security Considerations

### WebSocket Security
```python
# Origin validation
allowed_origins = ['http://localhost:8501', 'http://localhost:8502']

# Authentication headers
headers = {'Authorization': 'Bearer your-token-here'}

# Rate limiting
max_messages_per_minute = 1000
```

### Data Validation
```python
def validate_event(event_data: Dict) -> bool:
    required_fields = ['event_type', 'timestamp', 'game_id']
    return all(field in event_data for field in required_fields)
```

## Scaling Guidelines

### Horizontal Scaling
```yaml
# Docker Compose scaling
services:
  realtime-processor:
    scale: 3
  realtime-dashboard:
    scale: 2
```

### Load Balancing
```nginx
upstream realtime_backend {
    server realtime-processor-1:8765;
    server realtime-processor-2:8765;
    server realtime-processor-3:8765;
}

server {
    location /ws {
        proxy_pass http://realtime_backend;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

### Performance Tuning
```python
# Buffer sizes
WEBSOCKET_BUFFER_SIZE = 64 * 1024
STREAM_BUFFER_SIZE = 10000
WINDOW_RETENTION_HOURS = 2

# Thread configuration
STREAM_WORKERS = min(8, os.cpu_count())
WEBSOCKET_WORKERS = min(4, os.cpu_count())

# Memory limits
MAX_EVENTS_IN_MEMORY = 50000
MAX_WINDOWS_PER_TYPE = 100
```

## API Reference

### Real-time Processor API
- `start()`: Start WebSocket server and event processing
- `stop()`: Gracefully shutdown all services
- `get_stats()`: Return processing statistics
- `submit_event(event)`: Add event to processing queue

### Stream Processor API
- `register_processor(event_type, func)`: Register event handler
- `create_tumbling_window(event_type, duration)`: Create fixed windows
- `create_sliding_window(event_type, duration, slide)`: Create overlapping windows
- `submit_event(event)`: Submit event for processing

### WebSocket Message Format
```typescript
interface WebSocketMessage {
    event_type: string;
    timestamp: string;
    game_id: string;
    home_team?: string;
    away_team?: string;
    data: Record<string, any>;
}
```

## Future Enhancements

### Planned Features
- Redis integration for distributed processing
- Kafka integration for enterprise event streaming
- Advanced ML-based event prediction
- Multi-sport support expansion
- Enhanced visualization components

### Performance Roadmap
- GPU-accelerated stream processing
- Edge computing deployment
- CDN integration for global distribution
- Advanced caching strategies

---

🔴 **Live System Ready!** The NFL Analytics Platform now supports enterprise-grade real-time processing with WebSocket integration, stream processing, live dashboards, and scalable event-driven architecture.