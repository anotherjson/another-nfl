"""
Dagster Asset Status Monitoring for Streamlit Integration.

Provides real-time monitoring of Dagster asset status, materialization times,
and pipeline health for intelligent caching and data freshness indicators.
"""

import streamlit as st
import requests
import json
import subprocess
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import time


@dataclass
class AssetStatus:
    """Status information for a Dagster asset."""
    asset_key: str
    status: str  # "MATERIALIZED", "NEVER_MATERIALIZED", "FAILED", etc.
    last_materialization: Optional[datetime]
    run_id: Optional[str]
    is_fresh: bool
    error_message: Optional[str] = None


@dataclass
class PipelineStatus:
    """Overall pipeline status information."""
    total_assets: int
    materialized_assets: int
    failed_assets: int
    fresh_assets: int
    last_run_time: Optional[datetime]
    overall_health: str  # "HEALTHY", "DEGRADED", "CRITICAL"


class DagsterMonitor:
    """
    Monitor Dagster asset status and pipeline health for Streamlit integration.
    
    Features:
    - Asset materialization status tracking
    - Real-time pipeline health monitoring
    - Integration with dbt staging models
    - Cache invalidation triggers
    - Job execution capabilities
    """
    
    def __init__(self, 
                 dagster_host: str = "localhost",
                 dagster_port: int = 3000,
                 freshness_threshold_hours: int = 24):
        self.dagster_host = dagster_host
        self.dagster_port = dagster_port
        self.base_url = f"http://{dagster_host}:{dagster_port}"
        self.freshness_threshold = timedelta(hours=freshness_threshold_hours)
        
        # Asset mappings between dbt models and Dagster assets
        self.dbt_to_dagster_mapping = {
            'stg_pbp': 'dbt_critical_staging_models',
            'stg_weekly': 'dbt_critical_staging_models', 
            'stg_team_desc': 'dbt_critical_staging_models',
            'stg_schedules': 'dbt_critical_staging_models',
            'stg_seasonal': 'dbt_high_priority_staging_models',
            'stg_players': 'dbt_high_priority_staging_models'
        }
    
    def _make_dagster_request(self, query: str, variables: Optional[Dict] = None) -> Optional[Dict]:
        """Make a GraphQL request to Dagster's API."""
        try:
            response = requests.post(
                f"{self.base_url}/graphql",
                json={
                    "query": query,
                    "variables": variables or {}
                },
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                st.warning(f"Dagster API request failed with status {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            # Dagster might not be running, which is okay for fallback mode
            return None
    
    def is_dagster_available(self) -> bool:
        """Check if Dagster web server is available."""
        try:
            response = requests.get(f"{self.base_url}/server_info", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def get_asset_status(self, asset_key: str) -> AssetStatus:
        """Get status for a specific Dagster asset."""
        query = """
        query GetAssetStatus($assetKey: AssetKeyInput!) {
            assetNodeOrError(assetKey: $assetKey) {
                ... on AssetNode {
                    assetKey {
                        path
                    }
                    assetMaterializations(limit: 1) {
                        materializationEvent {
                            timestamp
                            runId
                        }
                    }
                    freshnessInfo {
                        currentMinutesLate
                    }
                }
            }
        }
        """
        
        variables = {
            "assetKey": {"path": asset_key.split(".")}
        }
        
        result = self._make_dagster_request(query, variables)
        
        if not result or "data" not in result:
            return AssetStatus(
                asset_key=asset_key,
                status="UNKNOWN",
                last_materialization=None,
                run_id=None,
                is_fresh=False,
                error_message="Dagster API unavailable"
            )
        
        try:
            asset_data = result["data"]["assetNodeOrError"]
            
            if "assetMaterializations" in asset_data and asset_data["assetMaterializations"]:
                materialization = asset_data["assetMaterializations"][0]["materializationEvent"]
                last_materialization = datetime.fromtimestamp(materialization["timestamp"])
                run_id = materialization["runId"]
                status = "MATERIALIZED"
            else:
                last_materialization = None
                run_id = None
                status = "NEVER_MATERIALIZED"
            
            # Check freshness
            is_fresh = (last_materialization is not None and 
                       datetime.now() - last_materialization < self.freshness_threshold)
            
            return AssetStatus(
                asset_key=asset_key,
                status=status,
                last_materialization=last_materialization,
                run_id=run_id,
                is_fresh=is_fresh
            )
            
        except Exception as e:
            return AssetStatus(
                asset_key=asset_key,
                status="ERROR",
                last_materialization=None,
                run_id=None,
                is_fresh=False,
                error_message=str(e)
            )
    
    def get_dbt_model_dagster_status(self, dbt_model_name: str) -> AssetStatus:
        """Get Dagster asset status for a dbt model."""
        dagster_asset = self.dbt_to_dagster_mapping.get(dbt_model_name)
        
        if not dagster_asset:
            return AssetStatus(
                asset_key=dbt_model_name,
                status="NO_MAPPING",
                last_materialization=None,
                run_id=None,
                is_fresh=False,
                error_message=f"No Dagster asset mapping for {dbt_model_name}"
            )
        
        return self.get_asset_status(dagster_asset)
    
    def get_pipeline_status(self) -> PipelineStatus:
        """Get overall pipeline health status."""
        # Get status for all mapped assets
        asset_statuses = []
        
        for dbt_model, dagster_asset in self.dbt_to_dagster_mapping.items():
            status = self.get_asset_status(dagster_asset)
            asset_statuses.append(status)
        
        # Calculate summary metrics
        total_assets = len(asset_statuses)
        materialized_assets = sum(1 for s in asset_statuses if s.status == "MATERIALIZED")
        failed_assets = sum(1 for s in asset_statuses if s.status in ["FAILED", "ERROR"])
        fresh_assets = sum(1 for s in asset_statuses if s.is_fresh)
        
        # Get most recent materialization time
        last_times = [s.last_materialization for s in asset_statuses if s.last_materialization]
        last_run_time = max(last_times) if last_times else None
        
        # Determine overall health
        if failed_assets > 0:
            overall_health = "CRITICAL"
        elif fresh_assets < materialized_assets * 0.8:  # Less than 80% fresh
            overall_health = "DEGRADED" 
        else:
            overall_health = "HEALTHY"
        
        return PipelineStatus(
            total_assets=total_assets,
            materialized_assets=materialized_assets,
            failed_assets=failed_assets,
            fresh_assets=fresh_assets,
            last_run_time=last_run_time,
            overall_health=overall_health
        )
    
    def trigger_asset_materialization(self, asset_key: str) -> bool:
        """Trigger materialization of a specific asset."""
        mutation = """
        mutation LaunchAssetExecution($assetKeys: [AssetKeyInput!]!) {
            launchAssetExecution(
                executionParams: {
                    assetSelection: $assetKeys
                }
            ) {
                ... on LaunchRunSuccess {
                    run {
                        id
                        status
                    }
                }
                ... on LaunchRunFailure {
                    message
                }
            }
        }
        """
        
        variables = {
            "assetKeys": [{"path": asset_key.split(".")}]
        }
        
        result = self._make_dagster_request(mutation, variables)
        
        if result and "data" in result:
            execution_result = result["data"]["launchAssetExecution"]
            if "run" in execution_result:
                st.success(f"Triggered materialization for {asset_key}")
                return True
            else:
                st.error(f"Failed to trigger materialization: {execution_result.get('message', 'Unknown error')}")
                return False
        else:
            st.error(f"Failed to trigger materialization for {asset_key}")
            return False
    
    def trigger_dbt_model_refresh(self, dbt_model_name: str) -> bool:
        """Trigger refresh of a dbt model via its Dagster asset."""
        dagster_asset = self.dbt_to_dagster_mapping.get(dbt_model_name)
        
        if not dagster_asset:
            st.error(f"No Dagster asset mapping for dbt model {dbt_model_name}")
            return False
        
        return self.trigger_asset_materialization(dagster_asset)
    
    def execute_dagster_job(self, job_name: str) -> bool:
        """Execute a specific Dagster job."""
        mutation = """
        mutation LaunchRun($jobName: String!) {
            launchRun(
                executionParams: {
                    mode: "default"
                    selector: {
                        jobName: $jobName
                    }
                }
            ) {
                ... on LaunchRunSuccess {
                    run {
                        id
                        status
                    }
                }
                ... on LaunchRunFailure {
                    message
                }
            }
        }
        """
        
        variables = {"jobName": job_name}
        
        result = self._make_dagster_request(mutation, variables)
        
        if result and "data" in result:
            execution_result = result["data"]["launchRun"]
            if "run" in execution_result:
                st.success(f"Triggered job execution for {job_name}")
                return True
            else:
                st.error(f"Failed to execute job: {execution_result.get('message', 'Unknown error')}")
                return False
        else:
            st.error(f"Failed to execute job {job_name}")
            return False
    
    def get_recent_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent pipeline runs."""
        query = """
        query GetRecentRuns($limit: Int!) {
            runsOrError(limit: $limit) {
                ... on Runs {
                    results {
                        id
                        status
                        startTime
                        endTime
                        jobName
                        stats {
                            ... on RunStatsSnapshot {
                                materializations
                                expectations
                            }
                        }
                    }
                }
            }
        }
        """
        
        variables = {"limit": limit}
        result = self._make_dagster_request(query, variables)
        
        if result and "data" in result and "runsOrError" in result["data"]:
            runs_data = result["data"]["runsOrError"]
            if "results" in runs_data:
                return runs_data["results"]
        
        return []


# Streamlit integration functions
@st.cache_resource
def get_dagster_monitor():
    """Get cached DagsterMonitor instance."""
    return DagsterMonitor()


@st.cache_data(ttl=60)  # Cache for 1 minute
def get_pipeline_health_summary():
    """Get cached pipeline health summary."""
    monitor = get_dagster_monitor()
    
    if not monitor.is_dagster_available():
        return {
            "status": "UNAVAILABLE",
            "message": "Dagster web server is not available",
            "timestamp": datetime.now().isoformat()
        }
    
    pipeline_status = monitor.get_pipeline_status()
    
    return {
        "status": "AVAILABLE",
        "pipeline_health": asdict(pipeline_status),
        "timestamp": datetime.now().isoformat()
    }


def should_refresh_model_cache(dbt_model_name: str) -> bool:
    """
    Determine if a dbt model cache should be refreshed based on Dagster asset status.
    
    Returns True if:
    - Dagster asset was recently materialized
    - Model hasn't been queried since last materialization
    - Asset is stale according to Dagster freshness policy
    """
    monitor = get_dagster_monitor()
    
    if not monitor.is_dagster_available():
        # If Dagster unavailable, use conservative refresh policy
        return True
    
    asset_status = monitor.get_dbt_model_dagster_status(dbt_model_name)
    
    # Refresh if asset is fresh (recently materialized)
    if asset_status.is_fresh and asset_status.last_materialization:
        # Check if materialization is more recent than our last cache
        # This would be compared against cache timestamps in real implementation
        return True
    
    # Refresh if asset has never been materialized or failed
    if asset_status.status in ["NEVER_MATERIALIZED", "FAILED", "ERROR"]:
        return True
    
    return False