# NFL dbt Staging Explorer

## Overview

The NFL dbt Staging Explorer is a single-page Streamlit application designed exclusively for exploring dbt staging tables. It provides a clean, focused interface for examining the structure and content of materialized dbt staging models.

## Features

### ✅ Pure dbt Focus
- **Only shows actual dbt staging tables** - no fallback data sources
- **Smart deduplication** - shows one table per type (prioritizes enhanced versions)
- **Materialization guidance** - clear instructions when tables don't exist

### 🏗️ Available Tables
The explorer automatically detects and displays:
- **PBP (Play-by-Play)**: `stg_pbp_enhanced` or working pbp table
- **Weekly Stats**: `stg_weekly_enhanced` or `stg_weekly`
- **Team Descriptions**: `stg_team_desc_enhanced` or `stg_team_desc_ducklake` or `stg_team_desc`
- **Schedules**: `stg_schedules_enhanced` or `stg_schedules`

### 📊 Core Functionality
- **Table Selection**: Sidebar dropdown with clean table names
- **Schema Explorer**: Column definitions and data types
- **Data Preview**: Configurable row limits (100-2000 rows)
- **Smart Filters**: Dynamic filtering by team, season, week, position
- **Data Export**: CSV download with timestamps
- **Quick Analytics**: Table-specific visualizations and summaries

## Usage

### Starting the Explorer
```bash
# From project root
uv run streamlit run pure_staging_explorer.py --server.port 8504
```

### Access
- **URL**: http://localhost:8504
- **Interface**: Single page, no navigation
- **Sidebar**: Table selection only

### If No Tables Found
The explorer will show materialization instructions:

**Via Dagster Web UI:**
```bash
uv run dagster dev -f nfl_dagster/definitions.py
# Navigate to http://localhost:3000 and materialize staging assets
```

**Via Dagster CLI:**
```bash
uv run dagster asset materialize --select tag:staging
```

**Via dbt:**
```bash
cd dbt
uv run dbt run --select tag:staging
```

## Technical Implementation

### Architecture
- **File**: `pure_staging_explorer.py`
- **Database**: DuckDB (`data/nfl_analytics.duckdb`)
- **Framework**: Streamlit single-page application
- **Caching**: 5-minute TTL for performance

### Special Handling
- **PBP Table Fix**: Automatically creates `pbp_working` table when `stg_pbp` has path issues
- **Table Validation**: Tests accessibility before showing tables
- **Error Handling**: Graceful failures with informative messages

### Data Sources
- **Primary**: dbt staging models in DuckDB
- **Fallback**: Working views with correct parquet paths (PBP only)
- **No Parquet Fallbacks**: Forces proper dbt materialization

## Table-Specific Analytics

### Team Descriptions
- Conference/division distributions
- Team counts by grouping

### Weekly Stats  
- Position distributions
- Season coverage analysis
- Fantasy points histograms

### Schedules
- Games per week trends
- Game type distributions
- Scoring patterns

### Play-by-Play
- Play type distributions
- Down situation analysis
- EPA metrics

## File Structure
```
pure_staging_explorer.py       # Main application
├── DbtStagingExplorer        # Core explorer class
├── Table detection logic     # Smart deduplication
├── Data loading & caching    # Performance optimization
├── Filter system            # Dynamic data filtering  
├── Analytics engine         # Table-specific insights
└── Export functionality     # CSV download
```

## Development Notes

### Design Principles
1. **Single Purpose**: Only explore dbt staging tables
2. **No Fallbacks**: Force proper dbt materialization workflow
3. **Clean Interface**: Zero navigation complexity
4. **Smart Defaults**: Intelligent table prioritization

### Error Handling
- Tables must exist AND be accessible
- Clear error messages for broken tables
- Materialization guidance when tables missing
- Graceful degradation for analytics

### Performance
- Smart caching with TTL
- Row limits for large tables
- Efficient table existence checks
- Minimal resource usage

---

*This explorer is designed to validate and examine the dbt staging layer, ensuring proper data pipeline execution and staging model quality.*