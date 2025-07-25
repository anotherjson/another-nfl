# NFL Staging Models

This directory contains the production staging models for the NFL analytics pipeline. These models provide clean, standardized data optimized for visualization and analytics.

## Production Models (Clean)

### Core Staging Models
- **`stg_pbp`** - Play-by-play data with advanced metrics and visualization-ready categorizations
- **`stg_weekly`** - Weekly player statistics with fantasy analytics and performance tiers  
- **`stg_team_desc`** - Team descriptions with colors, logos, and display-ready formatting
- **`stg_schedules`** - Game schedules with competitiveness metrics and categorizations

## Model Features

### Enhanced Analytics Fields
All staging models include calculated fields optimized for Streamlit dashboards:
- **Categorizations**: Performance tiers, outcome categories, competitiveness levels
- **Display Fields**: Human-readable labels, color codes, formatted names
- **Sorting**: Numeric sort orders for conferences, divisions, and rankings

### Data Quality
- **Comprehensive Testing**: 129 data tests across all models via `_staging_production.yml`
- **Referential Integrity**: Foreign key relationships between models
- **Data Validation**: Range checks, null constraints, and business rules

### Tags
- `staging` - Core staging layer models
- `streamlit_ready` - Optimized for visualization dashboards

## Usage

### dbt Commands
```bash
# Run all staging models
dbt run --select tag:staging

# Run specific model
dbt run --select stg_team_desc

# Test staging models
dbt test --select tag:staging
```

### Dagster Integration
Staging models are materialized as Dagster assets:
```bash
# Via Dagster CLI
dagster asset materialize --select dbt_staging_models

# Via web interface
# Navigate to localhost:3000 and materialize "dbt_staging" asset group
```

## Data Sources

All staging models source data from the DuckLake-managed raw data layer:
- Raw data extracted via Dagster assets
- Stored in partitioned parquet files: `data/{dataset}/{year}/etl_date={date}/data.parquet`
- Accessed via `create_staging_table_query()` macro for consistent data sourcing

## Configuration

### Materialization
- **Staging Models**: Materialized as `table` with indexes for performance
- **Enhanced Processing**: Custom macros handle data sourcing and transformations
- **DuckLake Integration**: Time-travel capabilities and versioning support

### Environment Variables
Required for proper operation:
- `DUCKLAKE_ATTACHMENT='ducklake'` - DuckLake database attachment
- `NFL_DATA_PATH='../data'` - Path to raw data directory

## Recent Changes (2025-07-25)

### Staging Model Cleanup
- **Removed Legacy Models**: Deleted broken models with path issues (`stg_pbp.sql`, `stg_weekly.sql`, etc.)
- **Renamed Production Models**: `stg_*_enhanced.sql` → `stg_*.sql` for cleaner naming
- **Cleaned Database**: Dropped legacy tables and views from DuckDB
- **Updated Configuration**: Consolidated to single `_staging_production.yml` configuration

### Benefits
- **Simplified Naming**: Clean, production-ready model names
- **Reduced Complexity**: Single source of truth for each dataset
- **Improved Maintainability**: Eliminated duplicate and broken models
- **Better Documentation**: Clear distinction between production and legacy components