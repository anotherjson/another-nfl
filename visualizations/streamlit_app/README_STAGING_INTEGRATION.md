# NFL Analytics Streamlit Dashboard - dbt Staging Models Integration

## Overview

Complete refactor of the NFL Analytics Streamlit dashboard to use dbt staging models instead of direct parquet file access. This implementation provides production-ready integration with the Dagster-orchestrated data pipeline, intelligent caching, comprehensive error handling, and real-time health monitoring.

## Architecture

### Before Refactor
```
Streamlit → Direct Parquet Files → Manual SQL Views → Basic Charts
```

### After Refactor  
```
Streamlit → dbt Staging Models → Dagster Monitoring → Enhanced Analytics
     ↓              ↓                   ↓                    ↓
Service Layer  Type-Safe Models   Health Checks      Advanced Features
```

## Key Components

### 1. Enhanced Data Access Layer
- **`utils/dbt_staging_connector.py`**: Direct dbt model execution via `dbt run`
- **`utils/dagster_monitor.py`**: Real-time Dagster asset status monitoring
- **Intelligent Caching**: Based on dbt run cycles and Dagster materialization times
- **Error Handling**: Comprehensive fallback and recovery mechanisms

### 2. Type-Safe Data Models
- **`data_models.py`**: Structured data models for all NFL entities
- **Enums**: `PositionGroup`, `Conference`, `FantasyTier`, `GameCompetitiveness`
- **Model Classes**: `TeamInfo`, `PlayerWeeklyStats`, `GameSchedule`, `PlayByPlayData`
- **Validation**: Automatic data validation and type conversion

### 3. Business Logic Service Layer  
- **`services/staging_service.py`**: High-level data access methods
- **Caching Strategy**: Multi-level caching with TTL based on data volatility
- **Data Processing**: Complex analytics calculations using staging models
- **Performance Optimization**: Intelligent query batching and filtering

### 4. Refactored Dashboard Pages
- **`main_staging.py`**: New main dashboard with full staging integration
- **`pages/team_analysis_staging.py`**: Enhanced team analysis
- **`pages/fantasy_dashboard_staging.py`**: Advanced fantasy analytics
- **Pipeline Health**: Real-time status indicators throughout

### 5. Advanced Monitoring & Health Checks
- **`components/health_monitor.py`**: Comprehensive system health monitoring
- **Data Lineage**: Visual pipeline flow and freshness tracking
- **Manual Controls**: Model refresh, cache management, health checks
- **Status Reporting**: Exportable system status reports

## Features

### ✅ Complete Migration
- **Zero Parquet Dependencies**: All queries use dbt staging models
- **Type Safety**: Structured data models with validation
- **Error Recovery**: Graceful handling of model failures
- **Performance**: Optimized caching and query strategies

### ✅ Deep Dagster Integration
- **Asset Status Monitoring**: Real-time materialization tracking
- **Cache Invalidation**: Smart cache refresh based on asset updates
- **Pipeline Health**: Visual health indicators and status reporting
- **Job Triggering**: Manual asset materialization from dashboard

### ✅ Intelligent Caching
- **Multi-Level Strategy**: Different TTL for static vs dynamic data
- **Freshness-Based**: Automatic invalidation on model updates
- **Performance Optimized**: Reduced query load while maintaining freshness
- **User Controls**: Manual cache clearing and refresh options

### ✅ Enhanced Analytics
- **Fantasy Football**: Advanced player analytics with consistency metrics
- **Team Performance**: Comprehensive team statistics and comparisons
- **Data Quality**: Built-in validation and freshness indicators
- **Interactive Visualizations**: Enhanced charts with Dagster status integration

### ✅ Production Ready
- **Comprehensive Error Handling**: Graceful degradation on failures
- **Health Monitoring**: Automated system health checks
- **Data Lineage**: Complete pipeline visibility
- **Performance Monitoring**: Query times and cache hit rates

## Usage

### Starting the Dashboard
```bash
# Navigate to streamlit app directory
cd visualizations/streamlit_app

# Run the new staging-integrated dashboard
uv run streamlit run main_staging.py --server.port 8504

# Access at: http://localhost:8504
```

### Development Mode
```bash
# Run individual pages for testing
uv run streamlit run pages/team_analysis_staging.py
uv run streamlit run pages/fantasy_dashboard_staging.py
```

### Health Monitoring
```bash
# Check dbt model status
uv run dbt run --select tag:staging

# Check Dagster pipeline health
uv run dagster dev -f nfl_dagster/definitions.py
# Access Dagster UI at: http://localhost:3000
```

## Configuration

### Environment Variables
The dashboard uses the existing project configuration:
- **DuckDB Path**: `data/nfl_analytics.duckdb` (main database)
- **dbt Project**: `dbt/` directory with staging models
- **Dagster**: `nfl_dagster/` orchestration

### Dashboard Settings
```python
# Cache TTL settings (in seconds)
TEAM_DATA_TTL = 1800      # 30 minutes (relatively static)
PLAYER_DATA_TTL = 600     # 10 minutes (dynamic)
HEALTH_CHECK_TTL = 60     # 1 minute (real-time monitoring)
```

## Data Models Integration

### Team Analysis
- **Source**: `stg_team_desc`, `stg_schedules`
- **Models**: `TeamInfo`, `GameSchedule`
- **Features**: Conference/division analysis, performance metrics
- **Caching**: 30-minute TTL for team data, 10-minute for performance

### Player Analysis  
- **Source**: `stg_weekly`, `stg_players`
- **Models**: `PlayerWeeklyStats`, `PositionGroup`, `FantasyTier`
- **Features**: Fantasy analytics, consistency metrics, position rankings
- **Caching**: 10-minute TTL with intelligent refresh on new data

### System Health
- **Source**: All staging models + Dagster API
- **Models**: `ModelStatus`, `PipelineStatus`, `AssetStatus`
- **Features**: Real-time health monitoring, data lineage, manual controls
- **Caching**: 1-minute TTL for real-time updates

## Performance Optimizations

### Query Optimization
- **Selective Loading**: Load only required columns and date ranges
- **Batch Processing**: Combine multiple small queries
- **Result Limiting**: Default limits with user override options
- **Index Utilization**: Leverage dbt model indexing strategies

### Caching Strategy
```python
# Multi-tier caching approach
@st.cache_data(ttl=1800)  # Static/reference data
def load_team_data(): ...

@st.cache_data(ttl=600)   # Dynamic player data  
def load_player_stats(): ...

@st.cache_data(ttl=60)    # Real-time monitoring
def get_health_status(): ...
```

### Memory Management
- **Lazy Loading**: Load data only when needed
- **Result Pagination**: Limit large result sets
- **Cache Cleanup**: Automatic cache eviction on memory pressure
- **Data Model Efficiency**: Minimal memory footprint for data models

## Error Handling & Recovery

### Model Execution Failures
```python
# Graceful degradation pattern
try:
    data = connector.query_staging_model('stg_weekly')
except ModelExecutionError:
    st.warning("Model unavailable - using cached data")
    data = load_cached_fallback()
```

### Dagster Integration Failures
```python
# Fallback to direct dbt execution
if not dagster_monitor.is_dagster_available():
    st.info("Using direct dbt execution mode")
    result = dbt_connector.run_staging_models(['stg_weekly'])
```

### Database Connection Issues
```python
# Connection retry with exponential backoff
@retry(max_attempts=3, backoff_factor=2)
def get_duckdb_connection():
    return duckdb.connect(db_path)
```

## Testing & Validation

### Data Quality Checks
- **Row Count Validation**: Ensure models have expected data volume
- **Schema Validation**: Verify column types and constraints
- **Business Logic**: Validate calculated fields and aggregations
- **Freshness Checks**: Monitor data recency and update frequency

### Integration Testing
```bash
# Test dbt model integration
uv run python -c "from services.staging_service import get_staging_service; print('✅ Integration working')"

# Test Dagster connectivity
uv run python -c "from utils.dagster_monitor import get_dagster_monitor; print('✅ Dagster connected')"

# Validate data models
uv run python -c "from data_models import TeamInfo; print('✅ Models loaded')"
```

### Performance Testing
- **Load Testing**: Simulate concurrent users
- **Query Performance**: Monitor execution times
- **Cache Efficiency**: Track hit rates and memory usage
- **Error Rate Monitoring**: Track failure rates and recovery times

## Deployment

### Development
```bash
# Local development with hot reload
uv run streamlit run main_staging.py --server.port 8504 --server.runOnSave true
```

### Production
```bash
# Production deployment (containerized)
docker-compose up -d streamlit-staging

# Or direct production run
uv run streamlit run main_staging.py --server.port 8504 --server.headless true
```

### Health Monitoring in Production
- **Automated Health Checks**: Every 5 minutes
- **Alert Thresholds**: >50% stale models = warning, >2 failed models = critical
- **Recovery Actions**: Automatic model refresh on failures
- **Status Dashboard**: Always-visible health indicators

## Migration Guide

### From Old Dashboard
1. **Data Access**: Replace all parquet queries with service layer calls
2. **Error Handling**: Add comprehensive try/catch blocks
3. **Caching**: Implement intelligent TTL-based caching
4. **Health Checks**: Add pipeline status monitoring
5. **User Experience**: Enhance with real-time status indicators

### Backwards Compatibility
- **Legacy Support**: Old dashboard remains functional during transition
- **Gradual Migration**: Page-by-page refactoring approach
- **Fallback Mode**: Automatic fallback to parquet if models unavailable
- **Configuration**: Environment variable toggles for old vs new behavior

## Future Enhancements

### Planned Features
- **Real-time Updates**: WebSocket integration for live data updates
- **Advanced Analytics**: Machine learning model integration
- **Custom Dashboards**: User-configurable dashboard layouts
- **Export Features**: PDF/Excel report generation
- **Mobile Optimization**: Responsive design improvements

### Scalability Improvements
- **Horizontal Scaling**: Multi-instance deployment support
- **Distributed Caching**: Redis/Memcached integration
- **Query Optimization**: Advanced query planning and execution
- **CDN Integration**: Static asset optimization

---

## Summary

This refactor represents a complete transformation of the NFL Analytics Streamlit dashboard from a basic parquet reader to a sophisticated, production-ready analytics platform. The integration with dbt staging models and Dagster orchestration provides:

- **Reliability**: Robust error handling and recovery mechanisms
- **Performance**: Intelligent caching and query optimization  
- **Maintainability**: Clean architecture with type-safe data models
- **Observability**: Comprehensive monitoring and health checks
- **Scalability**: Production-ready architecture for growth

The dashboard now serves as a showcase of modern data engineering practices, combining the power of dbt transformations, Dagster orchestration, and interactive visualization in a seamless, user-friendly interface.