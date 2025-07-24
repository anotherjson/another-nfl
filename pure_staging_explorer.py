"""
NFL dbt Staging Tables Explorer - Pure Implementation

Only shows actual dbt staging tables. If tables don't exist,
instructs user to materialize them via Dagster.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb
from typing import List, Dict, Any
from datetime import datetime

# Configure page
st.set_page_config(
    page_title="dbt Staging Explorer",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded"
)


class DbtStagingExplorer:
    """Pure dbt staging table explorer - no fallbacks."""
    
    def __init__(self, db_path: str = "data/nfl_analytics.duckdb"):
        self.db_path = db_path
        self._connection = None
    
    @st.cache_resource
    def get_connection(_self):
        """Get DuckDB connection."""
        if _self._connection is None:
            _self._connection = duckdb.connect(_self.db_path)
        return _self._connection
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query."""
        try:
            conn = self.get_connection()
            return conn.execute(query).df()
        except Exception as e:
            st.error(f"Query failed: {e}")
            return pd.DataFrame()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        try:
            conn = self.get_connection()
            result = conn.execute(f"""
                SELECT count(*) as cnt 
                FROM information_schema.tables 
                WHERE table_name = '{table_name.lower()}'
            """).fetchone()
            return result[0] > 0 if result else False
        except:
            return False
    
    def get_dbt_staging_tables(self) -> List[str]:
        """Get all actual dbt staging tables from the database."""
        try:
            conn = self.get_connection()
            
            # Get all tables starting with 'stg_'
            result = conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name LIKE 'stg_%'
                AND table_schema = 'main'
                ORDER BY table_name
            """).fetchall()
            
            staging_tables = [row[0] for row in result]
            
            # Filter out duplicates and test tables, prioritize certain versions
            unique_tables = []
            seen_base_names = set()
            
            # Define priority order (enhanced > ducklake > base)
            priority_suffixes = ['_enhanced', '_ducklake', '']
            
            # First pass: get all base names
            all_base_names = set()
            for table in staging_tables:
                if table.startswith('stg_'):
                    base_name = table.replace('_enhanced', '').replace('_ducklake', '')
                    all_base_names.add(base_name)
            
            # Second pass: select highest priority version of each base table
            for base_name in sorted(all_base_names):
                for suffix in priority_suffixes:
                    candidate = base_name + suffix
                    if candidate in staging_tables and self.table_is_accessible(candidate):
                        unique_tables.append(candidate)
                        break
            
            # Add any remaining staging tables that don't fit the pattern
            for table in staging_tables:
                if table not in unique_tables and self.table_is_accessible(table):
                    # Check if it's not just a variant of an existing table
                    base_name = table.replace('_enhanced', '').replace('_ducklake', '')
                    if base_name not in [t.replace('_enhanced', '').replace('_ducklake', '') for t in unique_tables]:
                        unique_tables.append(table)
            
            return sorted(unique_tables)
            
        except Exception as e:
            st.error(f"Error getting staging tables: {e}")
            return []
    
    def ensure_working_pbp_table(self):
        """Ensure we have a working pbp table."""
        try:
            conn = self.get_connection()
            # Always recreate the working pbp table
            conn.execute("DROP VIEW IF EXISTS pbp_working")
            conn.execute("""
                CREATE VIEW pbp_working AS
                SELECT 
                    game_id,
                    play_id,
                    season,
                    week,
                    posteam as team,
                    down,
                    ydstogo as yards_to_go,
                    play_type,
                    yards_gained,
                    COALESCE(epa, 0) as epa,
                    'Play-by-Play Data' as data_source
                FROM 'data/pbp/*/etl_date=*/data.parquet'
                WHERE season >= 2023
                LIMIT 2000
            """)
        except Exception:
            pass  # If it fails, we'll just not have pbp data
    
    def table_is_accessible(self, table_name: str) -> bool:
        """Check if table exists and is accessible (can run queries)."""
        try:
            conn = self.get_connection()
            # Try a simple count query to verify the table works
            conn.execute(f"SELECT count(*) FROM {table_name} LIMIT 1").fetchone()
            return True
        except Exception:
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table metadata."""
        try:
            conn = self.get_connection()
            
            # Get columns
            columns_df = conn.execute(f"DESCRIBE {table_name}").df()
            
            # Get row count
            row_count = conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            
            # Get sample for data freshness
            sample_df = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").df()
            
            return {
                "columns": columns_df,
                "row_count": row_count,
                "sample_data": sample_df,
                "exists": True
            }
        except Exception as e:
            return {
                "error": str(e),
                "exists": False
            }


@st.cache_resource
def get_explorer():
    return DbtStagingExplorer()


@st.cache_data(ttl=300)
def load_table_data(table_name: str, limit: int) -> pd.DataFrame:
    """Load table data with caching."""
    explorer = get_explorer()
    return explorer.execute_query(f"SELECT * FROM {table_name} LIMIT {limit}")


def main():
    """Pure dbt staging explorer."""
    st.title("🏗️ dbt Staging Tables Explorer")
    st.markdown("**Explore dbt staging models materialized by Dagster**")
    
    explorer = get_explorer()
    
    # Get dbt staging tables
    with st.spinner("Checking for dbt staging tables..."):
        staging_tables = explorer.get_dbt_staging_tables()
    
    # Show data source status with expected vs actual tables
    expected_staging_models = [
        'stg_pbp', 'stg_weekly', 'stg_schedules', 'stg_team_desc',
        'stg_seasonal', 'stg_players', 'stg_weekly_rosters', 'stg_seasonal_rosters',
        'stg_qbr', 'stg_injuries', 'stg_depth_charts', 'stg_snap_counts', 'stg_ngs_data',
        'stg_weekly_pfr', 'stg_seasonal_pfr', 'stg_ftn_data', 'stg_officials', 
        'stg_combine', 'stg_draft_picks'
    ]
    
    if staging_tables:
        # Map found tables to expected base names
        found_base_names = set()
        for table in staging_tables:
            base_name = table.replace('_enhanced', '').replace('_ducklake', '')
            found_base_names.add(base_name)
        
        expected_base_names = set(expected_staging_models)
        missing_models = expected_base_names - found_base_names
        
        if not missing_models:
            st.success(f"✅ **Complete Coverage: {len(staging_tables)} staging tables found** - All 19 NFL datasets covered!")
        else:
            st.warning(f"⚠️ **Found {len(staging_tables)} staging tables** - Missing {len(missing_models)} models: {', '.join(sorted(missing_models))}")
        
        # Show summary
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Available Models", len(staging_tables))
        with col2:
            st.metric("🎯 Expected Models", len(expected_staging_models))
        with col3:
            coverage_pct = (len(found_base_names) / len(expected_base_names)) * 100
            st.metric("✅ Coverage", f"{coverage_pct:.0f}%")
            
    else:
        st.error("❌ **No dbt staging tables found**")
        st.markdown("""
        ### 🔧 To materialize dbt staging tables:
        
        **Option 1: Via Dagster Web UI**
        ```bash
        uv run dagster dev -f nfl_dagster/definitions.py
        # Navigate to http://localhost:3000
        # Materialize staging assets
        ```
        
        **Option 2: Via Dagster CLI**
        ```bash
        # Materialize all staging models
        uv run dagster asset materialize --select tag:staging
        
        # Or materialize specific staging assets
        uv run dagster asset materialize --select dbt_critical_staging_models
        ```
        
        **Option 3: Via dbt directly**
        ```bash
        cd dbt
        uv run dbt run --select tag:staging
        ```
        """)
        return
    
    # Sidebar for table selection
    st.sidebar.title("📊 dbt Staging Tables")
    selected_table = st.sidebar.selectbox(
        "Select a staging table:",
        staging_tables,
        format_func=lambda x: x.replace("stg_", "").replace("_enhanced", "").replace("_ducklake", "").title()
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Available Tables:**")
    for table in staging_tables:
        clean_name = table.replace("stg_", "").replace("_enhanced", "").replace("_ducklake", "")
        st.sidebar.markdown(f"• {clean_name.title()}")
    
    if not selected_table:
        st.info("👈 Select a staging table from the sidebar")
        return
    
    # Show table details
    show_staging_table(explorer, selected_table)


def show_staging_table(explorer: DbtStagingExplorer, table_name: str):
    """Show complete staging table exploration."""
    
    st.header(f"📋 {table_name}")
    
    # Get table info
    with st.spinner("Loading table information..."):
        table_info = explorer.get_table_info(table_name)
    
    if not table_info.get("exists"):
        st.error(f"Cannot access table: {table_info.get('error', 'Unknown error')}")
        return
    
    # Metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📊 Rows", f"{table_info.get('row_count', 0):,}")
    
    with col2:
        columns_df = table_info.get("columns", pd.DataFrame())
        st.metric("📋 Columns", len(columns_df))
    
    with col3:
        # Check for dbt metadata
        sample_df = table_info.get("sample_data", pd.DataFrame())
        if "dbt_loaded_at" in sample_df.columns and not sample_df.empty:
            st.metric("🏗️ Source", "dbt Model")
        else:
            st.metric("🏗️ Source", "Staging Table")
    
    # Schema
    if not columns_df.empty:
        st.subheader("📋 Table Schema")
        st.dataframe(columns_df, use_container_width=True)
    
    # Data exploration
    st.subheader("🔍 Data Explorer")
    
    # Controls
    col1, col2 = st.columns([3, 1])
    with col1:
        row_limit = st.selectbox("Rows to display:", [100, 500, 1000, 2000], index=1)
    with col2:
        if st.button("🔄 Refresh Data"):
            load_table_data.clear()
    
    # Load data
    with st.spinner(f"Loading {row_limit} rows..."):
        df = load_table_data(table_name, row_limit)
    
    if df.empty:
        st.warning("No data in this staging table")
        return
    
    # Filters
    filtered_df = apply_staging_filters(df)
    
    # Data display
    st.subheader(f"📊 Staging Data ({len(filtered_df)} rows)")
    st.dataframe(filtered_df, use_container_width=True)
    
    # Download
    if not filtered_df.empty:
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            "📥 Download CSV",
            csv,
            f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "text/csv"
        )
    
    # Analytics
    show_staging_analytics(filtered_df, table_name)


def apply_staging_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Apply filters to staging data."""
    if df.empty:
        return df
    
    st.write("**🎛️ Filters:**")
    
    # Common staging table filters
    if "team" in df.columns:
        teams = ["All"] + sorted(df["team"].dropna().unique().tolist())
        selected_team = st.selectbox("Team:", teams)
        if selected_team != "All":
            df = df[df["team"] == selected_team]
    
    if "season" in df.columns:
        seasons = ["All"] + sorted(df["season"].dropna().unique().tolist(), reverse=True)
        selected_season = st.selectbox("Season:", seasons)
        if selected_season != "All":
            df = df[df["season"] == selected_season]
    
    if "week" in df.columns:
        weeks = ["All"] + sorted(df["week"].dropna().unique().tolist())
        selected_week = st.selectbox("Week:", weeks)
        if selected_week != "All":
            df = df[df["week"] == selected_week]
    
    if "position" in df.columns:
        positions = ["All"] + sorted(df["position"].dropna().unique().tolist())
        selected_position = st.selectbox("Position:", positions)
        if selected_position != "All":
            df = df[df["position"] == selected_position]
    
    return df


def show_staging_analytics(df: pd.DataFrame, table_name: str):
    """Show analytics for staging data."""
    if df.empty:
        return
    
    st.subheader("📈 Staging Data Analytics")
    
    # Table-specific analytics
    if "team_desc" in table_name:
        show_team_staging_analytics(df)
    elif "weekly" in table_name:
        show_weekly_staging_analytics(df)
    elif "schedules" in table_name:
        show_schedule_staging_analytics(df)
    elif "pbp" in table_name:
        show_pbp_staging_analytics(df)
    else:
        show_generic_staging_analytics(df)


def show_team_staging_analytics(df: pd.DataFrame):
    """Team staging table analytics."""
    col1, col2 = st.columns(2)
    
    with col1:
        if "team_conf" in df.columns or "conference" in df.columns:
            conf_col = "team_conf" if "team_conf" in df.columns else "conference"
            st.subheader("Conference Distribution")
            conf_counts = df[conf_col].value_counts()
            fig = px.pie(values=conf_counts.values, names=conf_counts.index)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if "team_division" in df.columns or "division" in df.columns:
            div_col = "team_division" if "team_division" in df.columns else "division"
            st.subheader("Division Distribution")
            div_counts = df[div_col].value_counts()
            fig = px.bar(x=div_counts.values, y=div_counts.index, orientation='h')
            st.plotly_chart(fig, use_container_width=True)


def show_weekly_staging_analytics(df: pd.DataFrame):
    """Weekly staging table analytics."""
    col1, col2 = st.columns(2)
    
    with col1:
        if "position" in df.columns:
            st.subheader("Position Distribution")
            pos_counts = df["position"].value_counts()
            fig = px.bar(x=pos_counts.values, y=pos_counts.index, orientation='h')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if "season" in df.columns:
            st.subheader("Data by Season")
            season_counts = df["season"].value_counts().sort_index()
            fig = px.bar(x=season_counts.index, y=season_counts.values)
            st.plotly_chart(fig, use_container_width=True)


def show_schedule_staging_analytics(df: pd.DataFrame):
    """Schedule staging table analytics."""
    col1, col2 = st.columns(2)
    
    with col1:
        if "week" in df.columns:
            st.subheader("Games by Week")
            week_counts = df["week"].value_counts().sort_index()
            fig = px.line(x=week_counts.index, y=week_counts.values)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if "game_type" in df.columns:
            st.subheader("Game Types")
            type_counts = df["game_type"].value_counts()
            fig = px.pie(values=type_counts.values, names=type_counts.index)
            st.plotly_chart(fig, use_container_width=True)


def show_pbp_staging_analytics(df: pd.DataFrame):
    """PBP staging table analytics."""
    col1, col2 = st.columns(2)
    
    with col1:
        if "play_type" in df.columns:
            st.subheader("Play Types")
            play_counts = df["play_type"].value_counts().head(8)
            fig = px.bar(x=play_counts.values, y=play_counts.index, orientation='h')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if "down" in df.columns:
            st.subheader("Down Distribution")
            down_counts = df["down"].value_counts().sort_index()
            fig = px.bar(x=down_counts.index, y=down_counts.values)
            st.plotly_chart(fig, use_container_width=True)


def show_generic_staging_analytics(df: pd.DataFrame):
    """Generic staging analytics."""
    # Numeric summary
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        with st.expander("📊 Numeric Columns Summary"):
            st.dataframe(df[numeric_cols].describe())
    
    # Categorical summary
    categorical_cols = df.select_dtypes(include=['object']).columns
    if len(categorical_cols) > 0:
        with st.expander("📋 Categorical Columns"):
            for col in categorical_cols[:3]:
                unique_count = df[col].nunique()
                st.write(f"**{col}**: {unique_count} unique values")


if __name__ == "__main__":
    main()