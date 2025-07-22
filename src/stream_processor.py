"""
Stream Processing Engine for NFL Real-time Data

This module provides high-performance stream processing capabilities for handling
large volumes of NFL data in real-time, including windowing, aggregations, and
complex event processing.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Callable, Union
from datetime import datetime, timedelta
from collections import deque, defaultdict
from dataclasses import dataclass, field
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from enum import Enum
import pandas as pd
import numpy as np
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WindowType(Enum):
    """Types of time windows for stream processing"""
    TUMBLING = "tumbling"  # Non-overlapping fixed windows
    SLIDING = "sliding"    # Overlapping fixed windows  
    SESSION = "session"    # Variable windows based on activity

@dataclass
class StreamEvent:
    """Base class for stream events"""
    timestamp: datetime
    event_type: str
    data: Dict[str, Any]
    partition_key: str = ""
    
    def __post_init__(self):
        if not self.partition_key:
            self.partition_key = f"{self.event_type}_{self.timestamp.timestamp()}"

@dataclass
class Window:
    """Time window for stream processing"""
    start_time: datetime
    end_time: datetime
    window_type: WindowType
    events: List[StreamEvent] = field(default_factory=list)
    
    def add_event(self, event: StreamEvent) -> bool:
        """Add event to window if it fits"""
        if self.start_time <= event.timestamp < self.end_time:
            self.events.append(event)
            return True
        return False
    
    def get_duration(self) -> timedelta:
        """Get window duration"""
        return self.end_time - self.start_time
    
    def is_complete(self, watermark: datetime) -> bool:
        """Check if window is complete based on watermark"""
        return watermark >= self.end_time

class StreamProcessor:
    """High-performance stream processing engine"""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.windows: Dict[str, List[Window]] = defaultdict(list)
        self.processors: Dict[str, Callable] = {}
        self.watermark = datetime.now()
        self.event_queue = deque(maxlen=10000)
        self.is_running = False
        self.stats = {
            'events_processed': 0,
            'windows_processed': 0,
            'errors': 0,
            'start_time': None
        }
        
    def register_processor(self, event_type: str, processor_func: Callable[[List[StreamEvent]], Any]) -> None:
        """Register a processor function for specific event types"""
        self.processors[event_type] = processor_func
        logger.info(f"Registered processor for {event_type}")
    
    def start(self) -> None:
        """Start the stream processor"""
        if self.is_running:
            logger.warning("Stream processor is already running")
            return
            
        logger.info("Starting stream processor...")
        self.is_running = True
        self.stats['start_time'] = datetime.now()
        
        # Start background tasks
        self.executor.submit(self._event_processor_loop)
        self.executor.submit(self._window_processor_loop)
        self.executor.submit(self._watermark_updater_loop)
        
        logger.info("✅ Stream processor started successfully")
    
    def stop(self) -> None:
        """Stop the stream processor"""
        logger.info("Stopping stream processor...")
        self.is_running = False
        self.executor.shutdown(wait=True)
        logger.info("✅ Stream processor stopped")
    
    def submit_event(self, event: StreamEvent) -> None:
        """Submit event for processing"""
        self.event_queue.append(event)
    
    def create_tumbling_window(self, event_type: str, duration: timedelta) -> None:
        """Create tumbling windows for event type"""
        now = datetime.now()
        window_start = now.replace(second=0, microsecond=0)
        
        # Create initial windows
        for i in range(5):  # Create next 5 windows
            start = window_start + (i * duration)
            end = start + duration
            window = Window(start, end, WindowType.TUMBLING)
            self.windows[event_type].append(window)
        
        logger.info(f"Created tumbling windows for {event_type} with {duration} duration")
    
    def create_sliding_window(self, event_type: str, duration: timedelta, slide: timedelta) -> None:
        """Create sliding windows for event type"""
        now = datetime.now()
        window_start = now.replace(second=0, microsecond=0)
        
        # Create overlapping windows
        for i in range(10):
            start = window_start + (i * slide)
            end = start + duration
            window = Window(start, end, WindowType.SLIDING)
            self.windows[event_type].append(window)
        
        logger.info(f"Created sliding windows for {event_type}: duration={duration}, slide={slide}")
    
    def _event_processor_loop(self) -> None:
        """Main event processing loop"""
        logger.info("Starting event processor loop...")
        
        while self.is_running:
            try:
                # Process events in batches for efficiency
                batch = []
                batch_size = min(100, len(self.event_queue))
                
                for _ in range(batch_size):
                    if self.event_queue:
                        batch.append(self.event_queue.popleft())
                
                if batch:
                    self._process_event_batch(batch)
                    self.stats['events_processed'] += len(batch)
                
                time.sleep(0.1)  # Small delay to prevent CPU spinning
                
            except Exception as e:
                logger.error(f"Event processor error: {e}")
                self.stats['errors'] += 1
                time.sleep(1)
    
    def _process_event_batch(self, events: List[StreamEvent]) -> None:
        """Process a batch of events"""
        # Group events by type for efficient processing
        events_by_type = defaultdict(list)
        for event in events:
            events_by_type[event.event_type].append(event)
        
        # Process each event type
        futures = []
        for event_type, event_list in events_by_type.items():
            future = self.executor.submit(self._assign_events_to_windows, event_type, event_list)
            futures.append(future)
        
        # Wait for all assignments to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error assigning events to windows: {e}")
                self.stats['errors'] += 1
    
    def _assign_events_to_windows(self, event_type: str, events: List[StreamEvent]) -> None:
        """Assign events to appropriate windows"""
        if event_type not in self.windows:
            # Auto-create tumbling windows if none exist
            self.create_tumbling_window(event_type, timedelta(minutes=5))
        
        for event in events:
            assigned = False
            for window in self.windows[event_type]:
                if window.add_event(event):
                    assigned = True
            
            if not assigned:
                # Create new window if event doesn't fit existing ones
                self._create_window_for_event(event_type, event)
    
    def _create_window_for_event(self, event_type: str, event: StreamEvent) -> None:
        """Create a new window to accommodate an event"""
        # Create a 5-minute tumbling window starting at event time
        window_start = event.timestamp.replace(second=0, microsecond=0)
        window_end = window_start + timedelta(minutes=5)
        
        window = Window(window_start, window_end, WindowType.TUMBLING)
        window.add_event(event)
        self.windows[event_type].append(window)
        
        logger.debug(f"Created new window for {event_type}: {window_start} - {window_end}")
    
    def _window_processor_loop(self) -> None:
        """Process completed windows"""
        logger.info("Starting window processor loop...")
        
        while self.is_running:
            try:
                for event_type in list(self.windows.keys()):
                    windows_to_process = []
                    windows_to_keep = []
                    
                    # Separate completed windows from active ones
                    for window in self.windows[event_type]:
                        if window.is_complete(self.watermark):
                            windows_to_process.append(window)
                        else:
                            windows_to_keep.append(window)
                    
                    # Update windows list
                    self.windows[event_type] = windows_to_keep
                    
                    # Process completed windows
                    if windows_to_process:
                        self._process_windows(event_type, windows_to_process)
                        self.stats['windows_processed'] += len(windows_to_process)
                
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Window processor error: {e}")
                self.stats['errors'] += 1
                time.sleep(10)
    
    def _process_windows(self, event_type: str, windows: List[Window]) -> None:
        """Process completed windows"""
        if event_type not in self.processors:
            logger.debug(f"No processor registered for {event_type}")
            return
        
        processor = self.processors[event_type]
        
        # Process windows in parallel
        futures = []
        for window in windows:
            future = self.executor.submit(self._process_single_window, processor, window)
            futures.append(future)
        
        # Wait for completion
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    logger.debug(f"Window processing result: {result}")
            except Exception as e:
                logger.error(f"Window processing error: {e}")
                self.stats['errors'] += 1
    
    def _process_single_window(self, processor: Callable, window: Window) -> Any:
        """Process a single window"""
        try:
            return processor(window.events)
        except Exception as e:
            logger.error(f"Error processing window: {e}")
            self.stats['errors'] += 1
            return None
    
    def _watermark_updater_loop(self) -> None:
        """Update watermark for determining window completion"""
        logger.info("Starting watermark updater loop...")
        
        while self.is_running:
            try:
                # Update watermark to current time minus some buffer for late events
                buffer = timedelta(minutes=2)
                self.watermark = datetime.now() - buffer
                
                time.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                logger.error(f"Watermark updater error: {e}")
                time.sleep(60)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        runtime = datetime.now() - self.stats['start_time'] if self.stats['start_time'] else timedelta(0)
        
        return {
            **self.stats,
            'runtime_seconds': runtime.total_seconds(),
            'events_per_second': self.stats['events_processed'] / max(runtime.total_seconds(), 1),
            'active_windows': sum(len(windows) for windows in self.windows.values()),
            'watermark': self.watermark.isoformat()
        }

class NFLStreamAnalyzer:
    """Specialized NFL stream analyzer using the stream processor"""
    
    def __init__(self, processor: StreamProcessor):
        self.processor = processor
        self.game_states = {}
        self.player_stats = defaultdict(lambda: defaultdict(float))
        self.team_stats = defaultdict(lambda: defaultdict(float))
        
        # Register processors
        self._register_processors()
    
    def _register_processors(self) -> None:
        """Register NFL-specific processors"""
        
        def process_score_updates(events: List[StreamEvent]) -> Dict[str, Any]:
            """Process score update events"""
            results = {
                'total_events': len(events),
                'games_affected': set(),
                'total_points_scored': 0,
                'scoring_plays': []
            }
            
            for event in events:
                game_id = event.data.get('game_id')
                if game_id:
                    results['games_affected'].add(game_id)
                    
                    # Calculate points from score change
                    old_home = event.data.get('old_home_score', 0)
                    old_away = event.data.get('old_away_score', 0)
                    new_home = event.data.get('new_home_score', 0)
                    new_away = event.data.get('new_away_score', 0)
                    
                    points_scored = (new_home - old_home) + (new_away - old_away)
                    results['total_points_scored'] += points_scored
                    
                    if points_scored > 0:
                        results['scoring_plays'].append({
                            'game_id': game_id,
                            'points': points_scored,
                            'timestamp': event.timestamp.isoformat()
                        })
            
            results['games_affected'] = len(results['games_affected'])
            logger.info(f"Processed {results['total_events']} score updates, {results['total_points_scored']} total points")
            
            return results
        
        def process_fantasy_updates(events: List[StreamEvent]) -> Dict[str, Any]:
            """Process fantasy update events"""
            results = {
                'total_events': len(events),
                'players_affected': set(),
                'total_fantasy_points': 0.0,
                'top_performers': []
            }
            
            player_points = defaultdict(float)
            
            for event in events:
                players = event.data.get('fantasy_players_affected', [])
                point_changes = event.data.get('point_changes', {})
                
                for player_id in players:
                    results['players_affected'].add(player_id)
                    points = point_changes.get(player_id, 0)
                    player_points[player_id] += points
                    results['total_fantasy_points'] += points
            
            # Find top performers in this window
            sorted_players = sorted(player_points.items(), key=lambda x: x[1], reverse=True)
            results['top_performers'] = sorted_players[:10]
            results['players_affected'] = len(results['players_affected'])
            
            logger.info(f"Processed {results['total_events']} fantasy updates for {results['players_affected']} players")
            
            return results
        
        def process_play_updates(events: List[StreamEvent]) -> Dict[str, Any]:
            """Process play-by-play events"""
            results = {
                'total_events': len(events),
                'play_types': defaultdict(int),
                'yards_gained': defaultdict(float),
                'turnovers': 0
            }
            
            for event in events:
                play_type = event.data.get('play_type', 'unknown')
                results['play_types'][play_type] += 1
                
                yards = event.data.get('yards_gained', 0)
                results['yards_gained'][play_type] += yards
                
                if play_type in ['interception', 'fumble']:
                    results['turnovers'] += 1
            
            # Convert defaultdicts to regular dicts for JSON serialization
            results['play_types'] = dict(results['play_types'])
            results['yards_gained'] = dict(results['yards_gained'])
            
            logger.info(f"Processed {results['total_events']} plays, {results['turnovers']} turnovers")
            
            return results
        
        # Register processors with the stream processor
        self.processor.register_processor('score_update', process_score_updates)
        self.processor.register_processor('fantasy_update', process_fantasy_updates)
        self.processor.register_processor('play_update', process_play_updates)
        
        logger.info("Registered NFL stream processors")
    
    def create_analysis_windows(self) -> None:
        """Create analysis windows for different time scales"""
        
        # 1-minute tumbling windows for real-time analysis
        self.processor.create_tumbling_window('score_update', timedelta(minutes=1))
        self.processor.create_tumbling_window('fantasy_update', timedelta(minutes=1))
        self.processor.create_tumbling_window('play_update', timedelta(minutes=1))
        
        # 5-minute sliding windows with 1-minute slide for trend analysis
        self.processor.create_sliding_window('score_update', timedelta(minutes=5), timedelta(minutes=1))
        self.processor.create_sliding_window('fantasy_update', timedelta(minutes=5), timedelta(minutes=1))
        
        logger.info("Created NFL analysis windows")

async def simulate_live_events(processor: StreamProcessor, duration: int = 60) -> None:
    """Simulate live NFL events for testing"""
    logger.info(f"Simulating live events for {duration} seconds...")
    
    start_time = datetime.now()
    event_id = 0
    
    while (datetime.now() - start_time).seconds < duration:
        # Generate random events
        event_id += 1
        
        # Score update event
        score_event = StreamEvent(
            timestamp=datetime.now(),
            event_type='score_update',
            data={
                'game_id': f'game_{event_id % 3}',  # 3 games
                'old_home_score': (event_id % 20),
                'old_away_score': (event_id % 15),
                'new_home_score': (event_id % 20) + (1 if event_id % 10 == 0 else 0),
                'new_away_score': (event_id % 15) + (1 if event_id % 12 == 0 else 0),
            }
        )
        processor.submit_event(score_event)
        
        # Fantasy update event (less frequent)
        if event_id % 5 == 0:
            fantasy_event = StreamEvent(
                timestamp=datetime.now(),
                event_type='fantasy_update',
                data={
                    'game_id': f'game_{event_id % 3}',
                    'fantasy_players_affected': [f'player_{event_id % 20}', f'player_{(event_id + 1) % 20}'],
                    'point_changes': {
                        f'player_{event_id % 20}': round(np.random.normal(5, 2), 1),
                        f'player_{(event_id + 1) % 20}': round(np.random.normal(3, 1.5), 1)
                    }
                }
            )
            processor.submit_event(fantasy_event)
        
        # Play update event (most frequent)
        if event_id % 3 == 0:
            play_types = ['pass', 'rush', 'punt', 'field_goal', 'interception', 'fumble']
            play_event = StreamEvent(
                timestamp=datetime.now(),
                event_type='play_update',
                data={
                    'game_id': f'game_{event_id % 3}',
                    'play_type': np.random.choice(play_types, p=[0.4, 0.3, 0.1, 0.05, 0.05, 0.1]),
                    'yards_gained': np.random.randint(-5, 20),
                    'down': np.random.randint(1, 5),
                    'distance': np.random.randint(1, 15)
                }
            )
            processor.submit_event(play_event)
        
        await asyncio.sleep(0.5)  # Generate events every 0.5 seconds
    
    logger.info("Event simulation completed")

def main():
    """Demo of stream processing system"""
    logger.info("=== NFL Stream Processing Demo ===")
    
    # Initialize stream processor
    processor = StreamProcessor(max_workers=4)
    
    # Initialize NFL analyzer
    analyzer = NFLStreamAnalyzer(processor)
    analyzer.create_analysis_windows()
    
    try:
        # Start processing
        processor.start()
        
        logger.info("🚀 Stream processor running...")
        logger.info("📊 Simulating live NFL events...")
        
        # Run simulation
        asyncio.run(simulate_live_events(processor, duration=120))
        
        # Let processing complete
        time.sleep(10)
        
        # Show final stats
        stats = processor.get_stats()
        logger.info("📈 Final Processing Statistics:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        
    except KeyboardInterrupt:
        logger.info("Demo interrupted by user")
    finally:
        processor.stop()
        logger.info("✅ Demo completed")

if __name__ == "__main__":
    main()