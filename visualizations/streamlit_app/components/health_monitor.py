"""
Health monitoring components for the NFL Analytics Dashboard.

Provides comprehensive health check visualizations, data lineage tracking,
and system status monitoring for the dbt staging models integration.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from ..services.staging_service import get_staging_service
from ..utils.dagster_monitor import get_dagster_monitor, get_pipeline_health_summary
from ..data_models import dataframe_to_models


class HealthMonitor:
    """Comprehensive health monitoring for the NFL analytics platform."""
    
    def __init__(self):
        self.service = get_staging_service()
        self.dagster_monitor = get_dagster_monitor()
    
    def show_system_health_dashboard(self):
        """Display comprehensive system health dashboard."""
        st.header("🔧 System Health & Monitoring")
        
        # Get health data
        health_status = self.service.get_dashboard_health_status()
        pipeline_health = get_pipeline_health_summary()
        freshness_summary = self.service.get_data_freshness_summary()
        
        # Overall status cards
        self._show_status_cards(health_status, pipeline_health)
        
        # Health visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            self._show_model_health_chart(freshness_summary)
        
        with col2:
            self._show_pipeline_status_chart(pipeline_health)
        
        # Detailed health information
        self._show_detailed_health_table(freshness_summary)
        
        # Data lineage visualization
        self._show_data_lineage()
        
        # Manual controls
        self._show_manual_controls()
    
    def _show_status_cards(self, health_status: Dict[str, Any], pipeline_health: Dict[str, Any]):
        """Show high-level status cards."""
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            # Overall system health
            status_color = {
                'HEALTHY': '✅',
                'DEGRADED': '⚠️', 
                'CRITICAL': '❌'
            }
            
            st.metric(
                "System Health",
                health_status['status'],
                f"{status_color.get(health_status['status'], '❓')} {health_status['fresh_models']}/{health_status['total_models']} fresh"
            )
        
        with col2:
            # Dagster pipeline status
            if pipeline_health["status"] == "AVAILABLE":
                pipeline_data = pipeline_health["pipeline_health"]
                st.metric(
                    "Pipeline Status",
                    pipeline_data['overall_health'],
                    f"🔄 {pipeline_data['fresh_assets']}/{pipeline_data['total_assets']} assets"
                )
            else:
                st.metric("Pipeline Status", "UNAVAILABLE", "⚠️ Monitoring offline")
        
        with col3:
            # Data freshness
            fresh_models = health_status['fresh_models']
            total_models = health_status['total_models']
            freshness_pct = (fresh_models / total_models * 100) if total_models > 0 else 0
            
            st.metric(
                "Data Freshness",
                f"{freshness_pct:.0f}%",
                f"📊 {fresh_models} of {total_models} models"
            )
        
        with col4:
            # Error count
            error_models = health_status['error_models']
            st.metric(
                "Errors",
                error_models,
                "❌ Models with issues" if error_models > 0 else "✅ No errors"
            )
    
    def _show_model_health_chart(self, freshness_summary: Dict[str, Any]):
        """Show model health status chart."""
        st.subheader("📊 dbt Model Health")
        
        # Prepare data for visualization
        model_data = []
        for model_name, status in freshness_summary.items():
            dbt_status = status['dbt_status']
            
            health_score = 0
            if dbt_status['is_fresh']:
                health_score = 100
            elif dbt_status['error']:
                health_score = 0
            else:
                health_score = 50  # Stale but not errored
            
            model_data.append({
                'Model': model_name,
                'Health Score': health_score,
                'Status': 'Fresh' if dbt_status['is_fresh'] else ('Error' if dbt_status['error'] else 'Stale'),
                'Rows': dbt_status['row_count'] or 0
            })
        
        if model_data:
            df = pd.DataFrame(model_data)
            
            # Color mapping
            color_map = {'Fresh': '#00ff00', 'Stale': '#ffa500', 'Error': '#ff0000'}
            
            fig = px.bar(
                df,
                x='Model',
                y='Health Score', 
                color='Status',
                title="Model Health Status",
                color_discrete_map=color_map,
                hover_data=['Rows']
            )
            fig.update_xaxes(tickangle=45)
            fig.update_layout(yaxis_range=[0, 100])
            st.plotly_chart(fig, use_container_width=True)
    
    def _show_pipeline_status_chart(self, pipeline_health: Dict[str, Any]):
        """Show Dagster pipeline status chart."""
        st.subheader("🔄 Pipeline Status")
        
        if pipeline_health["status"] == "AVAILABLE":
            pipeline_data = pipeline_health["pipeline_health"]
            
            # Create donut chart for asset status
            labels = ['Fresh', 'Stale', 'Failed']
            values = [
                pipeline_data['fresh_assets'],
                pipeline_data['materialized_assets'] - pipeline_data['fresh_assets'],
                pipeline_data['failed_assets']
            ]
            
            colors = ['#00ff00', '#ffa500', '#ff0000']
            
            fig = go.Figure(data=[go.Pie(
                labels=labels,
                values=values,
                hole=.3,
                marker_colors=colors
            )])
            
            fig.update_traces(
                textposition='inside',
                textinfo='percent+label'
            )
            
            fig.update_layout(
                title="Dagster Asset Status",
                annotations=[dict(text=f"{pipeline_data['total_assets']}<br>Total Assets", x=0.5, y=0.5, font_size=12, showarrow=False)]
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Dagster pipeline monitoring unavailable")
            st.info("Using direct dbt execution mode")
    
    def _show_detailed_health_table(self, freshness_summary: Dict[str, Any]):
        """Show detailed health information table."""
        st.subheader("📋 Detailed Model Status")
        
        # Prepare detailed data
        detailed_data = []
        for model_name, status in freshness_summary.items():
            dbt_status = status['dbt_status']
            dagster_status = status['dagster_status']
            
            detailed_data.append({
                'Model': model_name,
                'dbt Status': '✅ Fresh' if dbt_status['is_fresh'] else ('❌ Error' if dbt_status['error'] else '⚠️ Stale'),
                'Row Count': dbt_status['row_count'] or 0,
                'Last dbt Run': dbt_status['last_run'][:19] if dbt_status['last_run'] else 'Never',
                'Dagster Status': dagster_status['status'],
                'Dagster Fresh': '✅' if dagster_status['is_fresh'] else '❌',
                'Last Materialization': dagster_status['last_materialization'][:19] if dagster_status['last_materialization'] else 'Never',
                'Error': dbt_status['error'] or dagster_status['error'] or 'None'
            })
        
        if detailed_data:
            df = pd.DataFrame(detailed_data)
            st.dataframe(df, use_container_width=True)
    
    def _show_data_lineage(self):
        """Show data lineage visualization."""
        st.subheader("📊 Data Lineage & Flow")
        
        st.write("**NFL Analytics Data Pipeline:**")
        
        # Create a flow diagram using text and arrows
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.markdown("""
            **🌐 NFL API**
            - nfl_data_py
            - 19 datasets
            - Raw JSON/CSV
            """)
        
        with col2:
            st.markdown("**→**")
            st.markdown("""
            **🔄 Dagster Assets**
            - Raw data extraction
            - DuckLake registration
            - Priority scheduling
            """)
        
        with col3:
            st.markdown("**→**")
            st.markdown("""
            **📊 dbt Staging**
            - Data cleaning
            - Standardization
            - Type safety
            """)
        
        with col4:
            st.markdown("**→**")
            st.markdown("""
            **🏗️ Intermediate**
            - Business logic
            - EPA calculations
            - Aggregations
            """)
        
        with col5:
            st.markdown("**→**")
            st.markdown("""
            **📱 Dashboard**
            - Streamlit UI
            - Interactive charts
            - Real-time updates
            """)
        
        # Data freshness timeline
        st.subheader("⏰ Data Freshness Timeline")
        
        freshness_summary = self.service.get_data_freshness_summary()
        timeline_data = []
        
        for model_name, status in freshness_summary.items():
            dbt_status = status['dbt_status']
            if dbt_status['last_run']:
                try:
                    last_run = datetime.fromisoformat(dbt_status['last_run'].replace('Z', '+00:00'))
                    timeline_data.append({
                        'Model': model_name,
                        'Last Updated': last_run,
                        'Hours Ago': (datetime.now() - last_run.replace(tzinfo=None)).total_seconds() / 3600,
                        'Status': 'Fresh' if dbt_status['is_fresh'] else 'Stale'
                    })
                except:
                    pass  # Skip invalid dates
        
        if timeline_data:
            timeline_df = pd.DataFrame(timeline_data)
            
            fig = px.scatter(
                timeline_df,
                x='Hours Ago',
                y='Model',
                color='Status',
                size_max=10,
                title="Model Freshness Timeline",
                labels={'Hours Ago': 'Hours Since Last Update'},
                color_discrete_map={'Fresh': '#00ff00', 'Stale': '#ffa500'}
            )
            fig.update_traces(marker=dict(size=12))
            st.plotly_chart(fig, use_container_width=True)
    
    def _show_manual_controls(self):
        """Show manual control panel."""
        st.subheader("🎛️ Manual Controls")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("🔄 Refresh All Models", help="Run all dbt staging models"):
                with st.spinner("Refreshing all staging models..."):
                    result = self.service.dbt_connector.refresh_all_staging_models()
                    
                    if result.success:
                        st.success(f"✅ Refreshed {len(result.models_executed)} models in {result.execution_time:.1f}s")
                        st.experimental_rerun()
                    else:
                        st.error(f"❌ Refresh failed: {result.error}")
        
        with col2:
            if st.button("🗑️ Clear Cache", help="Clear all Streamlit caches"):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.success("✅ Cache cleared successfully")
        
        with col3:
            if st.button("🔍 Check Health", help="Run comprehensive health check"):
                with st.spinner("Running health checks..."):
                    health_results = self._run_health_checks()
                    
                    if health_results['all_passed']:
                        st.success("✅ All health checks passed")
                    else:
                        st.warning(f"⚠️ {len(health_results['failures'])} health checks failed")
                        
                        for failure in health_results['failures']:
                            st.error(f"❌ {failure}")
        
        with col4:
            if st.button("📊 Export Status", help="Export system status report"):
                status_report = self._generate_status_report()
                
                st.download_button(
                    label="📥 Download Report",
                    data=status_report,
                    file_name=f"nfl_dashboard_status_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
    
    def _run_health_checks(self) -> Dict[str, Any]:
        """Run comprehensive health checks."""
        failures = []
        
        # Check dbt model health
        freshness_summary = self.service.get_data_freshness_summary()
        for model_name, status in freshness_summary.items():
            dbt_status = status['dbt_status']
            
            if dbt_status['error']:
                failures.append(f"Model {model_name} has error: {dbt_status['error']}")
            elif not dbt_status['is_fresh']:
                failures.append(f"Model {model_name} is stale")
            elif dbt_status['row_count'] == 0:
                failures.append(f"Model {model_name} has no data")
        
        # Check Dagster connectivity
        if not self.dagster_monitor.is_dagster_available():
            failures.append("Dagster web server is not available")
        
        # Check database connectivity
        try:
            conn = self.service.dbt_connector.get_duckdb_connection()
            conn.execute("SELECT 1").fetchone()
        except Exception as e:
            failures.append(f"DuckDB connection failed: {str(e)}")
        
        return {
            'all_passed': len(failures) == 0,
            'failures': failures,
            'total_checks': len(freshness_summary) + 2,  # models + dagster + duckdb
            'passed_checks': len(freshness_summary) + 2 - len(failures)
        }
    
    def _generate_status_report(self) -> str:
        """Generate comprehensive status report."""
        import json
        
        health_status = self.service.get_dashboard_health_status()
        pipeline_health = get_pipeline_health_summary()
        freshness_summary = self.service.get_data_freshness_summary()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'dashboard_version': 'dbt Staging Models Integration',
            'overall_health': health_status,
            'pipeline_health': pipeline_health,
            'model_details': freshness_summary,
            'system_info': {
                'dagster_available': self.dagster_monitor.is_dagster_available(),
                'total_models': len(freshness_summary),
                'fresh_models': sum(1 for s in freshness_summary.values() if s['dbt_status']['is_fresh']),
                'error_models': sum(1 for s in freshness_summary.values() if s['dbt_status']['error'])
            }
        }
        
        return json.dumps(report, indent=2, default=str)


# Streamlit component functions
def show_health_sidebar():
    """Show condensed health info in sidebar."""
    monitor = HealthMonitor()
    health_status = monitor.service.get_dashboard_health_status()
    
    st.sidebar.header("🔧 System Health")
    
    # Health indicator
    if health_status['status'] == 'HEALTHY':
        st.sidebar.success(f"✅ {health_status['message']}")
    elif health_status['status'] == 'DEGRADED':
        st.sidebar.warning(f"⚠️ {health_status['message']}")
    else:
        st.sidebar.error(f"❌ {health_status['message']}")
    
    # Quick stats
    st.sidebar.metric(
        "Fresh Models", 
        f"{health_status['fresh_models']}/{health_status['total_models']}"
    )
    
    if health_status['dagster_available']:
        st.sidebar.info("🔄 Dagster: Active")
    else:
        st.sidebar.warning("🔄 Dagster: Offline")
    
    # Quick refresh button
    if st.sidebar.button("🔄 Quick Refresh"):
        st.cache_data.clear()
        st.experimental_rerun()


def show_health_header():
    """Show health status in main header."""
    monitor = HealthMonitor()
    health_status = monitor.service.get_dashboard_health_status()
    pipeline_health = get_pipeline_health_summary()
    
    # Create status bar
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        if health_status['status'] == 'HEALTHY':
            st.success(f"✅ System: {health_status['status']} - {health_status['fresh_models']}/{health_status['total_models']} models fresh")
        elif health_status['status'] == 'DEGRADED':
            st.warning(f"⚠️ System: {health_status['status']} - {health_status['message']}")
        else:
            st.error(f"❌ System: {health_status['status']} - {health_status['message']}")
    
    with col2:
        if pipeline_health["status"] == "AVAILABLE":
            pipeline_data = pipeline_health["pipeline_health"]
            if pipeline_data['overall_health'] == 'HEALTHY':
                st.success(f"🔄 Pipeline: {pipeline_data['overall_health']}")
            else:
                st.warning(f"🔄 Pipeline: {pipeline_data['overall_health']}")
        else:
            st.info("🔄 Pipeline: Direct dbt mode")
    
    with col3:
        if st.button("🔧 Health Details"):
            st.session_state['show_health_modal'] = True