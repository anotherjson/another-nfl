"""
Real-time NFL Data Processing System

This module provides real-time data processing capabilities for live NFL games,
including event streaming, live score updates, and fantasy point calculations.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
import threading
import time
from dataclasses import dataclass, asdict
from enum import Enum
import websockets
import requests
from concurrent.futures import ThreadPoolExecutor
import duckdb
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventType(Enum):
    """Types of NFL events"""
    GAME_START = "game_start"
    GAME_END = "game_end" 
    SCORE_UPDATE = "score_update"
    PLAY_UPDATE = "play_update"
    FANTASY_UPDATE = "fantasy_update"
    INJURY_UPDATE = "injury_update"
    TIMEOUT = "timeout"
    QUARTER_END = "quarter_end"

@dataclass
class NFLEvent:
    """NFL real-time event data structure"""
    event_id: str
    event_type: EventType
    timestamp: datetime
    game_id: str
    home_team: str
    away_team: str
    data: Dict[str, Any]
    processed: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary"""
        result = asdict(self)
        result['timestamp'] = self.timestamp.isoformat()
        result['event_type'] = self.event_type.value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NFLEvent':
        """Create event from dictionary"""
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        data['event_type'] = EventType(data['event_type'])
        return cls(**data)

@dataclass
class LiveGame:
    """Live game state tracking"""
    game_id: str
    home_team: str
    away_team: str
    home_score: int = 0
    away_score: int = 0
    quarter: int = 1
    time_remaining: str = "15:00"
    possession: Optional[str] = None
    down_and_distance: Optional[str] = None
    field_position: Optional[str] = None
    is_active: bool = True
    last_update: datetime = None
    
    def __post_init__(self):
        if self.last_update is None:
            self.last_update = datetime.now()

class EventBuffer:
    """Thread-safe event buffer for real-time processing"""
    
    def __init__(self, max_size: int = 1000):
        self.events = []
        self.max_size = max_size
        self._lock = threading.Lock()
        
    def add_event(self, event: NFLEvent) -> None:
        """Add event to buffer"""
        with self._lock:
            self.events.append(event)
            if len(self.events) > self.max_size:
                self.events.pop(0)  # Remove oldest event
    
    def get_events(self, count: Optional[int] = None) -> List[NFLEvent]:
        """Get events from buffer"""
        with self._lock:
            if count is None:
                return self.events.copy()
            return self.events[-count:]
    
    def mark_processed(self, event_id: str) -> None:
        """Mark event as processed"""
        with self._lock:
            for event in self.events:
                if event.event_id == event_id:
                    event.processed = True
                    break
    
    def get_unprocessed(self) -> List[NFLEvent]:
        """Get unprocessed events"""
        with self._lock:
            return [event for event in self.events if not event.processed]

class NFLRealtimeProcessor:
    """Main real-time processing engine"""
    
    def __init__(self, db_path: str = "data/nfl_analytics.duckdb"):
        self.db_path = db_path
        self.event_buffer = EventBuffer()
        self.live_games: Dict[str, LiveGame] = {}
        self.subscribers: Dict[str, List[Callable]] = {
            event_type.value: [] for event_type in EventType
        }
        self.websocket_clients = set()
        self.is_running = False
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # Simulated data sources (in production, these would be real APIs)
        self.data_sources = {
            'espn': 'https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard',
            'nfl': 'https://api.nfl.com/v1/current/scores'  # Placeholder
        }
        
    def start(self) -> None:
        """Start the real-time processor"""
        logger.info("Starting NFL Real-time Processor...")
        self.is_running = True
        
        # Start background tasks
        self.executor.submit(self._game_monitor_loop)
        self.executor.submit(self._event_processor_loop)
        self.executor.submit(self._websocket_server)
        
        logger.info("✅ Real-time processor started successfully")
    
    def stop(self) -> None:
        """Stop the real-time processor"""
        logger.info("Stopping NFL Real-time Processor...")
        self.is_running = False
        self.executor.shutdown(wait=True)
        logger.info("✅ Real-time processor stopped")
    
    def subscribe(self, event_type: EventType, callback: Callable[[NFLEvent], None]) -> None:
        """Subscribe to specific event types"""
        self.subscribers[event_type.value].append(callback)
        logger.info(f"New subscriber for {event_type.value} events")
    
    def _publish_event(self, event: NFLEvent) -> None:
        """Publish event to subscribers"""
        self.event_buffer.add_event(event)
        
        # Notify subscribers
        for callback in self.subscribers[event.event_type.value]:
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Error in event callback: {e}")
        
        # Send to WebSocket clients
        asyncio.create_task(self._broadcast_to_websockets(event))
    
    async def _broadcast_to_websockets(self, event: NFLEvent) -> None:
        """Broadcast event to WebSocket clients"""
        if not self.websocket_clients:
            return
        
        message = json.dumps(event.to_dict(), default=str)
        disconnected = set()
        
        for websocket in self.websocket_clients:
            try:
                await websocket.send(message)
            except websockets.exceptions.ConnectionClosed:
                disconnected.add(websocket)
            except Exception as e:
                logger.error(f"WebSocket broadcast error: {e}")
                disconnected.add(websocket)
        
        # Remove disconnected clients
        self.websocket_clients -= disconnected
    
    def _game_monitor_loop(self) -> None:
        """Monitor live games and generate events"""
        logger.info("Starting game monitor loop...")
        
        while self.is_running:
            try:
                # Get current games (simulated data)
                live_games = self._fetch_live_games()
                
                for game_data in live_games:
                    game_id = game_data['game_id']
                    
                    # Update or create live game
                    if game_id not in self.live_games:
                        self.live_games[game_id] = LiveGame(
                            game_id=game_id,
                            home_team=game_data['home_team'],
                            away_team=game_data['away_team']
                        )
                        
                        # Generate game start event
                        event = NFLEvent(
                            event_id=f"game_start_{game_id}_{datetime.now().timestamp()}",
                            event_type=EventType.GAME_START,
                            timestamp=datetime.now(),
                            game_id=game_id,
                            home_team=game_data['home_team'],
                            away_team=game_data['away_team'],
                            data=game_data
                        )
                        self._publish_event(event)
                    
                    # Check for score updates
                    game = self.live_games[game_id]
                    if (game_data['home_score'] != game.home_score or 
                        game_data['away_score'] != game.away_score):
                        
                        # Generate score update event
                        event = NFLEvent(
                            event_id=f"score_update_{game_id}_{datetime.now().timestamp()}",
                            event_type=EventType.SCORE_UPDATE,
                            timestamp=datetime.now(),
                            game_id=game_id,
                            home_team=game.home_team,
                            away_team=game.away_team,
                            data={
                                'old_home_score': game.home_score,
                                'old_away_score': game.away_score,
                                'new_home_score': game_data['home_score'],
                                'new_away_score': game_data['away_score']
                            }
                        )
                        self._publish_event(event)
                        
                        # Update game state
                        game.home_score = game_data['home_score']
                        game.away_score = game_data['away_score']
                        game.last_update = datetime.now()
                
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Game monitor error: {e}")
                time.sleep(30)  # Wait longer on error
    
    def _event_processor_loop(self) -> None:
        """Process events and update database"""
        logger.info("Starting event processor loop...")
        
        while self.is_running:
            try:
                unprocessed_events = self.event_buffer.get_unprocessed()
                
                if unprocessed_events:
                    logger.info(f"Processing {len(unprocessed_events)} events...")
                    
                    for event in unprocessed_events:
                        self._process_event(event)
                        self.event_buffer.mark_processed(event.event_id)
                
                time.sleep(5)  # Process every 5 seconds
                
            except Exception as e:
                logger.error(f"Event processor error: {e}")
                time.sleep(10)
    
    def _process_event(self, event: NFLEvent) -> None:
        """Process individual event and update database"""
        try:
            conn = duckdb.connect(self.db_path)
            
            # Create real-time events table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS realtime_events (
                    event_id VARCHAR PRIMARY KEY,
                    event_type VARCHAR,
                    timestamp TIMESTAMP,
                    game_id VARCHAR,
                    home_team VARCHAR,
                    away_team VARCHAR,
                    data JSON,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Insert event
            conn.execute("""
                INSERT OR REPLACE INTO realtime_events 
                (event_id, event_type, timestamp, game_id, home_team, away_team, data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                event.event_id,
                event.event_type.value,
                event.timestamp,
                event.game_id,
                event.home_team,
                event.away_team,
                json.dumps(event.data)
            ])
            
            conn.close()
            logger.debug(f"Processed event: {event.event_id}")
            
        except Exception as e:
            logger.error(f"Error processing event {event.event_id}: {e}")
    
    def _fetch_live_games(self) -> List[Dict[str, Any]]:
        """Fetch live game data (simulated)"""
        # In production, this would fetch from real APIs
        # For demo purposes, we'll simulate some live games
        
        current_time = datetime.now()
        
        # Simulate 2-3 live games with changing scores
        games = [
            {
                'game_id': 'nfl_2024_week_1_kc_det',
                'home_team': 'DET',
                'away_team': 'KC',
                'home_score': int(current_time.second % 30),  # Simulate changing scores
                'away_score': int(current_time.minute % 25),
                'quarter': min(4, (current_time.minute % 60) // 15 + 1),
                'time_remaining': f"{15 - (current_time.second % 15)}:{current_time.second % 60:02d}",
                'status': 'in_progress'
            },
            {
                'game_id': 'nfl_2024_week_1_buf_mia',
                'home_team': 'MIA', 
                'away_team': 'BUF',
                'home_score': int(current_time.minute % 28),
                'away_score': int(current_time.second % 35),
                'quarter': min(4, (current_time.second % 60) // 15 + 1),
                'time_remaining': f"{14 - (current_time.minute % 15)}:{current_time.second % 60:02d}",
                'status': 'in_progress'
            }
        ]
        
        return games
    
    async def _websocket_server(self) -> None:
        """WebSocket server for real-time client connections"""
        async def handle_client(websocket, path):
            """Handle individual WebSocket client"""
            logger.info(f"New WebSocket client connected: {websocket.remote_address}")
            self.websocket_clients.add(websocket)
            
            try:
                # Send current live games on connect
                for game in self.live_games.values():
                    welcome_event = NFLEvent(
                        event_id=f"welcome_{game.game_id}_{datetime.now().timestamp()}",
                        event_type=EventType.SCORE_UPDATE,
                        timestamp=datetime.now(),
                        game_id=game.game_id,
                        home_team=game.home_team,
                        away_team=game.away_team,
                        data={
                            'home_score': game.home_score,
                            'away_score': game.away_score,
                            'quarter': game.quarter,
                            'time_remaining': game.time_remaining
                        }
                    )
                    await websocket.send(json.dumps(welcome_event.to_dict(), default=str))
                
                # Keep connection alive
                async for message in websocket:
                    # Handle client messages (e.g., subscriptions)
                    try:
                        data = json.loads(message)
                        logger.info(f"Received message from client: {data}")
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON from client: {message}")
                        
            except websockets.exceptions.ConnectionClosed:
                logger.info(f"WebSocket client disconnected: {websocket.remote_address}")
            except Exception as e:
                logger.error(f"WebSocket client error: {e}")
            finally:
                self.websocket_clients.discard(websocket)
        
        try:
            server = await websockets.serve(handle_client, "localhost", 8765)
            logger.info("🌐 WebSocket server started on ws://localhost:8765")
            await server.wait_closed()
        except Exception as e:
            logger.error(f"WebSocket server error: {e}")
    
    def get_live_games(self) -> List[Dict[str, Any]]:
        """Get current live games"""
        return [
            {
                'game_id': game.game_id,
                'home_team': game.home_team,
                'away_team': game.away_team,
                'home_score': game.home_score,
                'away_score': game.away_score,
                'quarter': game.quarter,
                'time_remaining': game.time_remaining,
                'is_active': game.is_active,
                'last_update': game.last_update
            }
            for game in self.live_games.values()
        ]
    
    def get_recent_events(self, count: int = 50) -> List[Dict[str, Any]]:
        """Get recent events"""
        recent_events = self.event_buffer.get_events(count)
        return [event.to_dict() for event in recent_events]

class FantasyLiveTracker:
    """Real-time fantasy points tracking"""
    
    def __init__(self, processor: NFLRealtimeProcessor):
        self.processor = processor
        self.player_scores: Dict[str, float] = {}
        
        # Subscribe to relevant events
        processor.subscribe(EventType.SCORE_UPDATE, self._handle_score_update)
        processor.subscribe(EventType.PLAY_UPDATE, self._handle_play_update)
    
    def _handle_score_update(self, event: NFLEvent) -> None:
        """Handle score update for fantasy calculations"""
        # In a real implementation, this would:
        # 1. Determine which player scored
        # 2. Calculate fantasy points for the score
        # 3. Update running totals
        # 4. Trigger fantasy update events
        
        logger.info(f"Processing fantasy implications of score update: {event.game_id}")
        
        # Simulate fantasy update
        fantasy_event = NFLEvent(
            event_id=f"fantasy_update_{event.game_id}_{datetime.now().timestamp()}",
            event_type=EventType.FANTASY_UPDATE,
            timestamp=datetime.now(),
            game_id=event.game_id,
            home_team=event.home_team,
            away_team=event.away_team,
            data={
                'fantasy_players_affected': ['player_123', 'player_456'],
                'point_changes': {'player_123': 6.0, 'player_456': 1.0}
            }
        )
        
        self.processor._publish_event(fantasy_event)
    
    def _handle_play_update(self, event: NFLEvent) -> None:
        """Handle play updates for fantasy calculations"""
        logger.info(f"Processing fantasy implications of play: {event.game_id}")
        # Similar implementation for play-by-play fantasy scoring

def main():
    """Demo of real-time processing system"""
    logger.info("=== NFL Real-time Processing Demo ===")
    
    # Initialize processor
    processor = NFLRealtimeProcessor()
    
    # Initialize fantasy tracker
    fantasy_tracker = FantasyLiveTracker(processor)
    
    # Example event subscribers
    def score_alert(event: NFLEvent):
        data = event.data
        logger.info(f"🏈 SCORE UPDATE: {event.home_team} {data.get('new_home_score', 0)} - {event.away_team} {data.get('new_away_score', 0)}")
    
    def fantasy_alert(event: NFLEvent):
        logger.info(f"⭐ FANTASY UPDATE: {event.data}")
    
    processor.subscribe(EventType.SCORE_UPDATE, score_alert)
    processor.subscribe(EventType.FANTASY_UPDATE, fantasy_alert)
    
    try:
        # Start processing
        processor.start()
        
        # Demo WebSocket client (in separate terminal):
        # python -c "import asyncio, websockets; asyncio.run(websockets.connect('ws://localhost:8765'))"
        
        logger.info("🚀 Real-time processor running... Press Ctrl+C to stop")
        logger.info("💻 WebSocket server available at: ws://localhost:8765")
        logger.info("📊 Connect with: websocat ws://localhost:8765")
        
        # Keep running
        while True:
            # Show live status
            live_games = processor.get_live_games()
            recent_events = processor.get_recent_events(5)
            
            logger.info(f"📺 Live games: {len(live_games)}")
            logger.info(f"📡 Recent events: {len(recent_events)}")
            logger.info(f"🔌 WebSocket clients: {len(processor.websocket_clients)}")
            
            time.sleep(30)
            
    except KeyboardInterrupt:
        logger.info("Stopping real-time processor...")
        processor.stop()
        logger.info("✅ Demo completed")

if __name__ == "__main__":
    main()