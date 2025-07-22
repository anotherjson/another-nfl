"""
Real-time NFL Dashboard with Live Updates

This dashboard connects to the real-time processing system to show live game updates,
fantasy scoring, and real-time analytics.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import asyncio
import websockets
import threading
import time
from datetime import datetime, timedelta
from queue import Queue
import logging

# Configure page
st.set_page_config(
    page_title="🔴 LIVE NFL Dashboard",
    page_icon="🏈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize session state
if 'live_data' not in st.session_state:
    st.session_state.live_data = {
        'games': {},
        'events': [],
        'fantasy_updates': [],
        'connection_status': 'Disconnected',
        'last_update': None
    }

if 'websocket_queue' not in st.session_state:
    st.session_state.websocket_queue = Queue()

if 'websocket_thread' not in st.session_state:
    st.session_state.websocket_thread = None

class WebSocketClient:
    """WebSocket client for real-time data"""
    
    def __init__(self, uri: str, queue: Queue):
        self.uri = uri
        self.queue = queue
        self.running = False
    
    async def connect_and_listen(self):
        """Connect to WebSocket and listen for messages"""
        try:
            async with websockets.connect(self.uri) as websocket:
                logger.info(f"Connected to WebSocket: {self.uri}")
                self.queue.put({'type': 'connection', 'status': 'Connected'})
                
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        self.queue.put({'type': 'event', 'data': data})
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse message: {e}")
                        
        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket connection closed")
            self.queue.put({'type': 'connection', 'status': 'Disconnected'})
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            self.queue.put({'type': 'connection', 'status': f'Error: {e}'})
    
    def start_in_thread(self):
        """Start WebSocket client in separate thread"""
        def run_websocket():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.connect_and_listen())
        
        thread = threading.Thread(target=run_websocket, daemon=True)
        thread.start()
        return thread

def process_websocket_messages():
    """Process incoming WebSocket messages"""
    messages_processed = 0
    
    while not st.session_state.websocket_queue.empty():
        message = st.session_state.websocket_queue.get()
        messages_processed += 1
        
        if message['type'] == 'connection':
            st.session_state.live_data['connection_status'] = message['status']
            
        elif message['type'] == 'event':
            event_data = message['data']
            event_type = event_data.get('event_type')
            
            # Update games data
            if event_type in ['score_update', 'game_start']:
                game_id = event_data.get('game_id')
                st.session_state.live_data['games'][game_id] = {
                    'game_id': game_id,
                    'home_team': event_data.get('home_team'),
                    'away_team': event_data.get('away_team'),
                    'home_score': event_data.get('data', {}).get('new_home_score', 0),
                    'away_score': event_data.get('data', {}).get('new_away_score', 0),
                    'last_update': datetime.now(),
                    'status': 'LIVE'
                }
            
            # Store fantasy updates
            if event_type == 'fantasy_update':
                st.session_state.live_data['fantasy_updates'].insert(0, {
                    'timestamp': datetime.now(),
                    'game_id': event_data.get('game_id'),
                    'data': event_data.get('data', {})
                })
                # Keep only last 50 updates
                st.session_state.live_data['fantasy_updates'] = st.session_state.live_data['fantasy_updates'][:50]
            
            # Store all events
            st.session_state.live_data['events'].insert(0, event_data)
            # Keep only last 100 events
            st.session_state.live_data['events'] = st.session_state.live_data['events'][:100]
            
            st.session_state.live_data['last_update'] = datetime.now()
    
    return messages_processed

def main():
    """Main dashboard application"""
    
    st.title("🔴 LIVE NFL Dashboard")
    st.markdown("### Real-time game updates, scores, and fantasy tracking")
    
    # Sidebar for connection management
    st.sidebar.title("🔌 Live Connection")
    
    websocket_uri = st.sidebar.text_input(
        "WebSocket URI", 
        value="ws://localhost:8765",
        help="URL of the real-time NFL processing server"
    )
    
    # Connection controls
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if st.button("🟢 Connect"):
            if st.session_state.websocket_thread is None or not st.session_state.websocket_thread.is_alive():
                client = WebSocketClient(websocket_uri, st.session_state.websocket_queue)
                st.session_state.websocket_thread = client.start_in_thread()
                st.success("Connecting...")
                time.sleep(1)
                st.experimental_rerun()
            else:
                st.warning("Already connected!")
    
    with col2:
        if st.button("🔴 Disconnect"):
            st.session_state.live_data['connection_status'] = 'Disconnected'
            st.info("Disconnected from live feed")
    
    # Process incoming messages
    messages_processed = process_websocket_messages()
    
    # Connection status
    status = st.session_state.live_data['connection_status']
    if status == 'Connected':
        st.sidebar.success(f"✅ {status}")
    elif 'Error' in status:
        st.sidebar.error(f"❌ {status}")
    else:
        st.sidebar.warning(f"⚠️ {status}")
    
    # Last update info
    last_update = st.session_state.live_data.get('last_update')
    if last_update:
        st.sidebar.info(f"🕐 Last update: {last_update.strftime('%H:%M:%S')}")
    
    # Auto-refresh controls
    st.sidebar.markdown("---")
    auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh", value=True)
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 1, 30, 5)
    
    if auto_refresh:
        time.sleep(refresh_interval)
        st.experimental_rerun()
    
    # Manual refresh
    if st.sidebar.button("🔄 Refresh Now"):
        st.experimental_rerun()
    
    # Main dashboard content
    live_games = st.session_state.live_data['games']
    recent_events = st.session_state.live_data['events']
    fantasy_updates = st.session_state.live_data['fantasy_updates']
    
    # Live games section
    st.header("🏈 Live Games")
    
    if live_games:
        # Create games display
        game_cols = st.columns(min(len(live_games), 3))
        
        for i, (game_id, game) in enumerate(live_games.items()):
            with game_cols[i % 3]:
                with st.container():
                    st.markdown(f"**{game['away_team']} @ {game['home_team']}**")
                    
                    # Score display
                    col1, col2, col3 = st.columns([1, 1, 1])
                    with col1:
                        st.metric(game['away_team'], game['away_score'])
                    with col2:
                        st.markdown("<div style='text-align: center; font-size: 20px;'>-</div>", unsafe_allow_html=True)
                    with col3:
                        st.metric(game['home_team'], game['home_score'])
                    
                    # Game status
                    if game.get('status') == 'LIVE':
                        st.success("🔴 LIVE")
                    else:
                        st.info("📺 In Progress")
                    
                    # Last update
                    if 'last_update' in game:
                        time_ago = (datetime.now() - game['last_update']).seconds
                        st.caption(f"Updated {time_ago}s ago")
    else:
        st.info("🔍 No live games currently. Start the real-time processor to see live data!")
        st.code("python -m src.realtime_processor")
    
    # Real-time statistics
    st.header("📊 Live Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Live Games", len(live_games))
    
    with col2:
        st.metric("Recent Events", len(recent_events))
    
    with col3:
        st.metric("Fantasy Updates", len(fantasy_updates))
    
    with col4:
        st.metric("Messages Processed", messages_processed)
    
    # Recent events feed
    st.header("📡 Live Event Feed")
    
    if recent_events:
        # Create scrollable event feed
        with st.container():
            for i, event in enumerate(recent_events[:20]):  # Show last 20 events
                event_time = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
                time_ago = (datetime.now() - event_time.replace(tzinfo=None)).seconds
                
                event_type = event.get('event_type', 'unknown')
                game_id = event.get('game_id', 'N/A')
                
                # Event styling based on type
                if event_type == 'score_update':
                    emoji = "🏈"
                    color = "green"
                elif event_type == 'fantasy_update':
                    emoji = "⭐"
                    color = "blue"
                elif event_type == 'game_start':
                    emoji = "🚀"
                    color = "orange"
                else:
                    emoji = "📊"
                    color = "gray"
                
                with st.expander(f"{emoji} {event_type.replace('_', ' ').title()} - {time_ago}s ago"):
                    col1, col2 = st.columns([1, 2])
                    
                    with col1:
                        st.write(f"**Game:** {game_id}")
                        st.write(f"**Type:** {event_type}")
                        st.write(f"**Time:** {event_time.strftime('%H:%M:%S')}")
                    
                    with col2:
                        st.json(event.get('data', {}))
    else:
        st.info("📭 No events received yet. Connect to the live feed to see real-time updates.")
    
    # Fantasy tracking section
    st.header("⭐ Live Fantasy Tracking")
    
    if fantasy_updates:
        fantasy_df = pd.DataFrame([
            {
                'Time': update['timestamp'].strftime('%H:%M:%S'),
                'Game': update['game_id'],
                'Players': len(update['data'].get('fantasy_players_affected', [])),
                'Total Points': sum(update['data'].get('point_changes', {}).values())
            }
            for update in fantasy_updates[:10]
        ])
        
        st.dataframe(fantasy_df, use_container_width=True)
        
        # Fantasy points chart
        if len(fantasy_updates) > 1:
            fig = px.line(
                fantasy_df, 
                x='Time', 
                y='Total Points',
                title="Live Fantasy Points Changes",
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("🎯 No fantasy updates yet. Fantasy tracking will appear here during live games.")
    
    # Debug information
    with st.expander("🔧 Debug Information"):
        st.write("**Session State:**")
        st.json({
            'connection_status': st.session_state.live_data['connection_status'],
            'games_count': len(live_games),
            'events_count': len(recent_events),
            'fantasy_updates_count': len(fantasy_updates),
            'websocket_thread_alive': st.session_state.websocket_thread.is_alive() if st.session_state.websocket_thread else False
        })
    
    # Instructions
    st.markdown("---")
    st.markdown("""
    ### 📋 How to Use Live Dashboard
    
    1. **Start Real-time Processor**: `python -m src.realtime_processor`
    2. **Connect**: Click "Connect" button in sidebar
    3. **Watch Live**: See games, scores, and events update in real-time
    4. **Fantasy Tracking**: Monitor live fantasy point changes
    
    ### 🔧 Technical Notes
    
    - WebSocket connection provides real-time updates
    - Auto-refresh keeps the dashboard current
    - All events are stored and displayed chronologically
    - Fantasy calculations happen in real-time during games
    """)

if __name__ == "__main__":
    main()