# Enhanced dbt-Streamlit Integration Guide

## Overview

This guide covers the enhanced integration between the NFL data extraction pipeline, dbt staging models, Dagster orchestration, and Streamlit dashboard that provides seamless data flow and improved user experience.

## 🏗️ Architecture Overview

```
NFL API Extraction → Parquet Files → Enhanced dbt Models → DuckDB Tables → Streamlit Dashboard
                                        ↓
                    Dagster Orchestration → Cache Management → Real-time Updates
```

### Key Enhancements

1. **Enhanced dbt Staging Models**: Optimized for visualization with calculated fields
2. **Direct DuckDB Integration**: Streamlit queries dbt models directly via DuckDB
3. **Automatic Cache Management**: Dagster triggers cache refresh after model updates
4. **Fallback Mechanisms**: Graceful degradation to raw parquet files if models unavailable
5. **Rich Metadata**: Models include dbt run information and data lineage

## 📊 Enhanced dbt Staging Models

### New Models

1. **`stg_pbp_enhanced`**: Play-by-play with visualization-friendly categories
2. **`stg_weekly_enhanced`**: Player stats with fantasy performance tiers
3. **`stg_team_desc_enhanced`**: Teams with chart colors and display names
4. **`stg_schedules_enhanced`**: Games with competitiveness and weather categories

### Key Features

- **Streamlit-Optimized**: Pre-calculated display fields and categories
- **Rich Metadata**: dbt run timestamps, model info, and source ETL dates
- **Performance Indexes**: Automatic index creation on key columns
- **Data Quality**: Built-in validation and deduplication
- **Color Mapping**: Team colors for consistent chart styling

### Sample Enhanced Fields

```sql
-- From stg_weekly_enhanced
case 
  when fantasy_points_ppr >= 20 then 'Elite (20+)'
  when fantasy_points_ppr >= 15 then 'Great (15-19.9)'
  when fantasy_points_ppr >= 10 then 'Good (10-14.9)'
  else 'Developing'
end as fantasy_performance_tier

-- From stg_team_desc_enhanced  
team_city || ' ' || team_name as full_team_name,
coalesce(team_color, '#808080') as chart_color_primary
```

## 🎨 Enhanced Streamlit Dashboard

### New Features

1. **dbt Staging Explorer**: Browse and analyze staging models
2. **Direct Model Queries**: Query staging tables with caching
3. **Pipeline Status**: Real-time dbt model and data status
4. **Enhanced Analytics**: Leverages pre-calculated fields from staging models
5. **Fallback Support**: Automatically falls back to parquet files if models unavailable

### Pages

1. **Overview**: Enhanced with pipeline status and dbt model information
2. **Team Analysis**: Uses `stg_team_desc_enhanced` with rich team metadata
3. **Player Stats**: Uses `stg_weekly_enhanced` with fantasy performance tiers
4. **Schedule Analysis**: Uses `stg_schedules_enhanced` with game competitiveness
5. **dbt Staging Explorer**: NEW - Interactive model browser and analyzer

### Connection Architecture

```python
# Enhanced data loading with fallback
def load_team_data_from_staging():
    try:
        # Try loading from dbt staging table
        df = query_staging_table("stg_team_desc_enhanced")
        return df
    except:
        # Fallback to raw parquet files
        return load_team_data_fallback()
```

## 🔄 Enhanced Dagster Integration

### New Assets

1. **`dbt_enhanced_staging_models`**: Builds all enhanced staging models
2. **`streamlit_data_refresh`**: Clears Streamlit cache after model updates
3. **`staging_data_quality_check`**: Validates data quality for dashboard consumption

### Orchestration Flow

```
Data Extraction Assets
        ↓
DuckLake Catalog Registration
        ↓
Enhanced dbt Staging Models
        ↓
Streamlit Cache Refresh
        ↓
Data Quality Validation
```

### Key Features

- **Automatic Cache Clearing**: Triggers Streamlit cache refresh after model builds
- **DuckLake Integration**: Registers new data files automatically
- **Quality Monitoring**: Validates staging models for dashboard readiness
- **Rich Metadata**: Tracks row counts, build status, and data freshness

## 🚀 Getting Started

### 1. Test Integration

```bash
# Run comprehensive integration test
uv run python scripts/test_enhanced_integration.py
```

### 2. Extract Sample Data

```bash
# Extract core datasets
uv run python -m src.cli extract dataset team_desc
uv run python -m src.cli extract dataset schedules --year 2024
uv run python -m src.cli extract dataset weekly --year 2024
```

### 3. Build Enhanced dbt Models

```bash
# Using CLI (recommended)
uv run python -m src.cli models materialize --verbose

# Or directly with dbt
cd dbt && uv run dbt run --select tag:streamlit_ready
```

### 4. Launch Enhanced Streamlit Dashboard

```bash
# Start enhanced dashboard
cd visualizations/streamlit_app
uv run streamlit run enhanced_main.py
```

### 5. Access Staging Explorer

Navigate to "dbt Staging Explorer" page in the Streamlit dashboard to:
- Browse available staging tables
- Analyze data quality and row counts
- Run interactive queries
- Download data as CSV
- View table schemas and metadata

## 🔧 Configuration

### Environment Variables

```bash
# DuckDB configuration
DUCKDB_DATABASE_PATH=data/nfl_analytics.duckdb

# PostgreSQL (for DuckLake)
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DATABASE=nfl_ducklake

# Data paths
NFL_DATA_PATH=data/
```

### dbt Configuration

Enhanced models use these tags:
- `streamlit_ready`: Models optimized for Streamlit
- `staging`: All staging models
- `enhanced`: Enhanced versions of models

### Streamlit Configuration

```python
# Cache settings
@st.cache_data(ttl=300)  # 5-minute cache
def query_staging_table(table_name, limit=1000):
    # Query implementation
```

## 📋 Usage Examples

### Query Enhanced Models via CLI

```bash
# List available models
uv run python -m src.cli models list

# Query specific model
uv run python -m src.cli models query stg_weekly_enhanced --limit 100

# Custom analytics query
uv run python -m src.cli models sql "
SELECT 
    position_group,
    COUNT(*) as player_count,
    AVG(fantasy_points_ppr) as avg_fantasy_points
FROM stg_weekly_enhanced 
WHERE season = 2024
GROUP BY position_group
ORDER BY avg_fantasy_points DESC
"
```

### Programmatic Access

```python
from visualizations.streamlit_app.utils.dbt_connection import (
    get_dbt_connection, 
    query_staging_table
)

# Get connection
conn = get_dbt_connection()

# Query staging table
df = query_staging_table("stg_team_desc_enhanced", limit=50)

# Custom query
custom_df = conn.execute_query("SELECT * FROM stg_weekly_enhanced WHERE position_group = 'Quarterback'")
```

### Dagster Operations

```bash
# Start Dagster UI
uv run dagster dev -f nfl_dagster/definitions.py

# Materialize enhanced staging models
uv run dagster asset materialize --asset dbt_enhanced_staging_models

# Refresh Streamlit data
uv run dagster asset materialize --asset streamlit_data_refresh
```

## 🔍 Troubleshooting

### Common Issues

1. **"No staging tables available"**
   - Run: `uv run python -m src.cli models materialize`
   - Check: Data extraction completed (`uv run python -m src.cli extract status`)

2. **"DuckDB connection failed"**
   - Verify: DuckDB extensions installed
   - Check: File permissions on `data/nfl_analytics.duckdb`

3. **"Streamlit cache not clearing"**
   - Manual clear: Restart Streamlit app
   - Check: Dagster asset `streamlit_data_refresh` succeeded

4. **"dbt models not found"**
   - Verify: Enhanced models in `dbt/models/staging/`
   - Run: `cd dbt && uv run dbt compile` to check for errors

### Debugging Steps

```bash
# 1. Check data availability
find data/ -name "*.parquet" | wc -l

# 2. Test DuckDB connection
uv run python -c "import duckdb; print(duckdb.connect('data/nfl_analytics.duckdb').execute('SELECT 1').fetchone())"

# 3. Test dbt compilation
cd dbt && uv run dbt compile --select tag:streamlit_ready

# 4. Test Streamlit utilities
uv run python -c "
from visualizations.streamlit_app.utils.dbt_connection import get_staging_table_summary
print(get_staging_table_summary())
"

# 5. Run integration test
uv run python scripts/test_enhanced_integration.py
```

## 📈 Performance Optimization

### dbt Model Performance

- **Materialization**: Tables with indexes for frequently queried models
- **Partitioning**: Year-based partitioning for large datasets
- **Incremental**: Models support incremental builds

### Streamlit Performance

- **Caching**: 5-minute TTL on data queries
- **Pagination**: Configurable row limits (100-2000)
- **Lazy Loading**: Data loaded only when needed

### DuckDB Performance

- **Extensions**: Pre-loaded parquet and httpfs extensions
- **Indexes**: Automatic indexes on game_id, player_id, team_abbr
- **Memory**: Configurable memory limits

## 🔄 Data Flow

### Complete Workflow

1. **Extract**: `uv run python -m src.cli extract dataset weekly --year 2024`
2. **Transform**: `uv run python -m src.cli models materialize`
3. **Analyze**: Open Streamlit dashboard → dbt Staging Explorer
4. **Monitor**: Check Dagster UI for pipeline status

### Automated Flow (Dagster)

1. Raw data assets extract from NFL API
2. DuckLake catalog registration updates
3. Enhanced dbt staging models build
4. Streamlit cache refresh triggers
5. Data quality validation completes
6. Dashboard shows updated data

## 🎯 Best Practices

### Development

1. **Test Integration**: Always run integration test after changes
2. **Model Updates**: Use `tag:streamlit_ready` for enhanced models
3. **Cache Management**: Clear Streamlit cache after dbt changes
4. **Error Handling**: Check both dbt build and Streamlit display

### Production

1. **Monitoring**: Use Dagster UI to monitor pipeline health
2. **Data Quality**: Review staging data quality checks
3. **Performance**: Monitor query times in Streamlit
4. **Backups**: Regular backups of DuckDB database

### User Experience

1. **Fallbacks**: Always provide fallback to raw data
2. **Status Display**: Show pipeline status in dashboard
3. **Loading States**: Use Streamlit spinners for long operations
4. **Error Messages**: Provide helpful error messages with next steps

---

## 🎉 Success Metrics

After implementing this enhanced integration:

- **Data Consistency**: Streamlit uses same transformed data as other analytics
- **Performance**: Pre-calculated fields reduce dashboard load times
- **User Experience**: Rich visualizations with proper categorization
- **Maintainability**: Single source of truth for staging transformations
- **Scalability**: Cached queries and indexed tables support growth

The enhanced integration provides a production-ready foundation for NFL analytics with seamless data flow from extraction through visualization.