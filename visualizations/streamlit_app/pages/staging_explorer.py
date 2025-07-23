"""
Streamlit page for exploring dbt staging tables.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils.dbt_connection import get_dbt_connection, query_staging_table, get_staging_table_summary


def show_staging_explorer():
    """Main staging explorer page."""
    st.title("🏗️ dbt Staging Tables Explorer")
    st.markdown("Explore and analyze the dbt staging models built from NFL data extraction pipeline.")
    
    # Get staging table summary
    with st.spinner("Loading staging table information..."):
        summary = get_staging_table_summary()
    
    if not summary["available_tables"]:
        st.warning("No staging tables available. Make sure dbt models have been materialized.")
        st.info("Run: `uv run python -m src.cli models materialize` to create staging tables.")
        return
    
    # Sidebar for table selection
    st.sidebar.title("Table Selection")
    selected_table = st.sidebar.selectbox(
        "Choose a staging table",
        summary["available_tables"],
        format_func=lambda x: x.replace("stg_", "").replace("_enhanced", "").title()
    )
    
    # Display table information
    show_table_info(selected_table, summary["table_info"].get(selected_table, {}))
    
    # Query and display data
    show_table_data(selected_table)
    
    # Table-specific analysis
    show_table_analysis(selected_table)


def show_table_info(table_name: str, table_info: dict):
    """Display table metadata and information."""
    st.header(f"📊 Table: {table_name}")
    
    if not table_info.get("table_exists", False):
        st.error(f"Table {table_name} does not exist or cannot be accessed.")
        return
    
    # Basic metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Rows", f"{table_info.get('row_count', 0):,}")
    
    with col2:
        columns_df = table_info.get("columns", pd.DataFrame())
        st.metric("Total Columns", len(columns_df))
    
    with col3:
        # Data freshness
        sample_df = table_info.get("sample_data", pd.DataFrame())
        if "dbt_loaded_at" in sample_df.columns and not sample_df.empty:
            last_update = sample_df["dbt_loaded_at"].max()
            st.metric("Last Updated", last_update.strftime("%Y-%m-%d %H:%M") if pd.notna(last_update) else "Unknown")  
        else:
            st.metric("Data Source", "dbt staging")
    
    # Column information
    if not columns_df.empty:
        st.subheader("📋 Table Schema")
        
        # Make column info more readable
        display_columns = columns_df.copy()
        if "column_type" in display_columns.columns:
            display_columns["column_type"] = display_columns["column_type"].str.replace("VARCHAR", "Text")
        
        st.dataframe(display_columns, use_container_width=True)


def show_table_data(table_name: str):
    """Display table data with filtering options."""
    st.header("🔍 Table Data")
    
    # Query options
    col1, col2 = st.columns([3, 1])
    
    with col1:
        row_limit = st.selectbox("Rows to display", [100, 500, 1000, 2000], index=1)
    
    with col2:
        refresh_data = st.button("🔄 Refresh Data")
    
    # Load data
    with st.spinner(f"Loading {row_limit} rows from {table_name}..."):
        if refresh_data:
            # Clear cache for this specific query
            query_staging_table.clear()
        
        df = query_staging_table(table_name, row_limit)
    
    if df.empty:
        st.warning("No data available in this table.")
        return
    
    # Data filtering
    if len(df) > 0:
        show_data_filters(df, table_name)
    
    # Display data
    st.subheader(f"Data Preview ({len(df)} rows)")
    st.dataframe(df, use_container_width=True)
    
    # Data download
    if not df.empty:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"{table_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )


def show_data_filters(df: pd.DataFrame, table_name: str):
    """Show data filtering options based on table content."""
    st.subheader("🎛️ Data Filters")
    
    # Common filters based on table type
    if "team" in df.columns:
        teams = ["All"] + sorted(df["team"].dropna().unique().tolist())
        selected_team = st.selectbox("Filter by Team", teams)
        if selected_team != "All":
            df = df[df["team"] == selected_team]
    
    if "season" in df.columns:
        seasons = ["All"] + sorted(df["season"].dropna().unique().tolist(), reverse=True)
        selected_season = st.selectbox("Filter by Season", seasons)
        if selected_season != "All":
            df = df[df["season"] == selected_season]
    
    if "week" in df.columns:
        weeks = ["All"] + sorted(df["week"].dropna().unique().tolist())
        selected_week = st.selectbox("Filter by Week", weeks)
        if selected_week != "All":
            df = df[df["week"] == selected_week]
    
    if "position" in df.columns or "position_group" in df.columns:
        pos_col = "position_group" if "position_group" in df.columns else "position"
        positions = ["All"] + sorted(df[pos_col].dropna().unique().tolist())
        selected_position = st.selectbox("Filter by Position", positions)
        if selected_position != "All":
            df = df[df[pos_col] == selected_position]
    
    return df


def show_table_analysis(table_name: str):
    """Show table-specific analysis and visualizations."""
    st.header("📈 Data Analysis")
    
    # Load full dataset for analysis
    with st.spinner("Loading data for analysis..."):
        df = query_staging_table(table_name, 5000)  # Larger sample for analysis
    
    if df.empty:
        st.warning("No data available for analysis.")
        return
    
    # Table-specific analysis
    if table_name == "stg_team_desc_enhanced":
        show_team_analysis(df)
    elif table_name == "stg_weekly_enhanced":
        show_weekly_analysis(df)
    elif table_name == "stg_schedules_enhanced":
        show_schedules_analysis(df)
    elif table_name == "stg_pbp_enhanced":
        show_pbp_analysis(df)
    else:
        show_generic_analysis(df)


def show_team_analysis(df: pd.DataFrame):
    """Analysis specific to team description data."""
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Teams by Conference")
        if "team_conference" in df.columns:
            conf_counts = df["team_conference"].value_counts()
            fig = px.pie(values=conf_counts.values, names=conf_counts.index, title="Conference Distribution")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Teams by Division")
        if "team_division" in df.columns:
            div_counts = df["team_division"].value_counts()
            fig = px.bar(x=div_counts.index, y=div_counts.values, title="Teams per Division")
            fig.update_xaxis(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)


def show_weekly_analysis(df: pd.DataFrame):
    """Analysis specific to weekly player statistics."""
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Players by Position Group")
        if "position_group" in df.columns:
            pos_counts = df["position_group"].value_counts()
            fig = px.bar(x=pos_counts.values, y=pos_counts.index, orientation='h', title="Player Count by Position")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Fantasy Performance Distribution")
        if "fantasy_performance_tier" in df.columns:
            tier_counts = df["fantasy_performance_tier"].value_counts()
            fig = px.pie(values=tier_counts.values, names=tier_counts.index, title="Fantasy Performance Tiers")
            st.plotly_chart(fig, use_container_width=True)
    
    # Top performers
    if "fantasy_points_ppr" in df.columns and "player_name" in df.columns:
        st.subheader("Top Fantasy Performers")
        top_performers = df.nlargest(10, "fantasy_points_ppr")[["player_name", "position_group", "team", "fantasy_points_ppr", "total_yards", "total_touchdowns"]]
        st.dataframe(top_performers, use_container_width=True)


def show_schedules_analysis(df: pd.DataFrame):
    """Analysis specific to schedule data."""
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Games by Week")
        if "week" in df.columns:
            week_counts = df["week"].value_counts().sort_index()
            fig = px.line(x=week_counts.index, y=week_counts.values, title="Games per Week")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Game Competitiveness")
        if "game_competitiveness" in df.columns:
            comp_counts = df["game_competitiveness"].value_counts()
            fig = px.bar(x=comp_counts.index, y=comp_counts.values, title="Game Competitiveness Distribution")
            fig.update_xaxis(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
    
    # Scoring analysis
    if "total_points" in df.columns:
        st.subheader("Scoring Analysis")
        col3, col4 = st.columns(2)
        
        with col3:
            avg_points = df["total_points"].mean()
            st.metric("Average Total Points", f"{avg_points:.1f}")
        
        with col4:
            fig = px.histogram(df, x="total_points", title="Distribution of Total Points Scored")
            st.plotly_chart(fig, use_container_width=True)


def show_pbp_analysis(df: pd.DataFrame):
    """Analysis specific to play-by-play data."""
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Play Outcome Categories")  
        if "play_outcome_category" in df.columns:
            outcome_counts = df["play_outcome_category"].value_counts()
            fig = px.pie(values=outcome_counts.values, names=outcome_counts.index, title="Play Outcomes")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Yardage Distribution")
        if "yardage_category" in df.columns:
            yard_counts = df["yardage_category"].value_counts()
            fig = px.bar(x=yard_counts.index, y=yard_counts.values, title="Yardage Categories")
            fig.update_xaxis(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)


def show_generic_analysis(df: pd.DataFrame):
    """Generic analysis for any table."""
    st.subheader("Data Summary")
    
    # Numeric columns summary
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        st.write("**Numeric Columns Summary:**")
        st.dataframe(df[numeric_cols].describe(), use_container_width=True)
    
    # Categorical columns summary
    categorical_cols = df.select_dtypes(include=['object']).columns
    if len(categorical_cols) > 0:
        st.write("**Categorical Columns Summary:**")
        for col in categorical_cols[:5]:  # Limit to first 5 to avoid clutter
            st.write(f"**{col}:** {df[col].nunique()} unique values")


if __name__ == "__main__":
    show_staging_explorer()