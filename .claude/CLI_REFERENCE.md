# NFL Analytics Platform - CLI Reference Guide

## Overview

The NFL Analytics Platform provides a comprehensive command-line interface (CLI) for managing NFL data extraction, transformation, and analysis. This reference guide covers all available commands, options, and usage patterns.

## Command Structure

```bash
uv run python -m src.cli <COMMAND_GROUP> <COMMAND> [OPTIONS] [ARGUMENTS]
```

## Command Groups

### 1. `explore` - Data Exploration Commands

Explore NFL datasets using `nfl_data_py` for initial data discovery.

#### `explore datasets`
List all available NFL datasets with metadata.

```bash
uv run python -m src.cli explore datasets
```

**Output**: Rich-formatted table showing:
- Dataset name
- Description
- Start year (when applicable)

#### `explore data <dataset>`
Show sample data from a specific NFL dataset.

```bash
uv run python -m src.cli explore data <dataset> [OPTIONS]
```

**Arguments**:
- `<dataset>`: Name of the NFL dataset (required)

**Options**:
- `--year <year>`: Year to fetch data for (required for year-based datasets)
- `--limit <number>`: Number of rows to display (default: 5)
- `--verbose, -v`: Show detailed error information

**Examples**:
```bash
# Basic team data exploration
uv run python -m src.cli explore data team_desc --limit 10

# Explore play-by-play data for 2023
uv run python -m src.cli explore data pbp --year 2023 --limit 5

# Weekly stats with error details
uv run python -m src.cli explore data weekly --year 2023 --verbose
```

### 2. `extract` - Production Data Extraction Commands

Production-grade data extraction with configuration, validation, and state management.

#### `extract dataset <dataset_name>`
Extract a single dataset with full production capabilities.

```bash
uv run python -m src.cli extract dataset <dataset_name> [OPTIONS]
```

**Arguments**:
- `<dataset_name>`: Name of the dataset to extract (required)

**Options**:
- `--year <year>`: Year to extract (required for year-based datasets)
- `--validate`: Validate extracted data (default: True)
- `--save`: Save to disk (default: True)  
- `--verbose, -v`: Show detailed extraction information

**Examples**:
```bash
# Extract team descriptions
uv run python -m src.cli extract dataset team_desc

# Extract 2023 play-by-play data
uv run python -m src.cli extract dataset pbp --year 2023 --verbose

# Extract without saving to disk
uv run python -m src.cli extract dataset weekly --year 2023 --no-save
```

#### `extract multiple <dataset_name>`
Extract multiple years of data at once.

```bash
uv run python -m src.cli extract multiple <dataset_name> [OPTIONS]
```

**Arguments**:
- `<dataset_name>`: Name of the dataset to extract (required)

**Options**:
- `--years <years>`: Comma-separated list of years (required)
- `--verbose, -v`: Show detailed extraction information

**Examples**:
```bash
# Extract multiple years of play-by-play data
uv run python -m src.cli extract multiple pbp --years 2020,2021,2022,2023

# Extract seasonal data with details
uv run python -m src.cli extract multiple seasonal --years 2018,2019,2020 --verbose
```

#### `extract incremental <dataset_name>`
Smart incremental extraction that skips already-extracted data.

```bash
uv run python -m src.cli extract incremental <dataset_name> [OPTIONS]
```

**Arguments**:
- `<dataset_name>`: Name of the dataset to extract (required)

**Options**:
- `--years <years>`: Comma-separated list of years
- `--max-age-days <days>`: Maximum age of data before refresh (default: 1)
- `--force`: Force refresh all data regardless of age
- `--verbose, -v`: Show detailed extraction information

**Examples**:
```bash
# Incremental extraction with default settings
uv run python -m src.cli extract incremental pbp

# Custom age threshold and specific years
uv run python -m src.cli extract incremental weekly --years 2020,2021,2022 --max-age-days 7

# Force refresh all data
uv run python -m src.cli extract incremental schedules --force
```

#### `extract status [dataset_name]`
Show extraction status and statistics.

```bash
uv run python -m src.cli extract status [dataset_name] [OPTIONS]
```

**Arguments**:
- `[dataset_name]`: Specific dataset to check (optional)

**Options**:
- `--verbose, -v`: Show detailed status information

**Examples**:
```bash
# Overall extraction status
uv run python -m src.cli extract status

# Status for specific dataset
uv run python -m src.cli extract status pbp --verbose
```

#### `extract cleanup`
Clean up old extraction files.

```bash
uv run python -m src.cli extract cleanup [OPTIONS]
```

**Options**:
- `--max-age-days <days>`: Maximum age of files to keep (default: 30)
- `--dry-run`: Show what would be deleted without deleting
- `--verbose, -v`: Show detailed cleanup information

**Examples**:
```bash
# Clean up files older than 30 days
uv run python -m src.cli extract cleanup

# Preview cleanup without deleting
uv run python -m src.cli extract cleanup --dry-run --verbose

# Custom age threshold
uv run python -m src.cli extract cleanup --max-age-days 7
```

### 3. `read` - Parquet File Reading

Direct parquet file reading and analysis.

#### `read <file_path>`
Read and display parquet file contents.

```bash
uv run python -m src.cli read <file_path> [OPTIONS]
```

**Arguments**:
- `<file_path>`: Path to parquet file (required)

**Options**:
- `--limit <number>`: Number of rows to display
- `--info`: Show file metadata information

**Examples**:
```bash
# Read parquet file
uv run python -m src.cli read data/team_desc/etl_date=2025-01-01/data.parquet

# Show file information
uv run python -m src.cli read data/sample.parquet --info --limit 10
```

### 4. `models` - dbt Model Operations (NEW)

Advanced dbt model management through DuckLake integration.

#### `models list`
List all available dbt staging models.

```bash
uv run python -m src.cli models list
```

**Output**: Rich-formatted table showing:
- Model name
- Description  
- Schema
- Base table

#### `models materialize`
Trigger dbt staging model materialization via Dagster.

```bash
uv run python -m src.cli models materialize [OPTIONS]
```

**Options**:
- `--verbose, -v`: Show detailed materialization output

**Examples**:
```bash
# Basic materialization
uv run python -m src.cli models materialize

# With detailed output
uv run python -m src.cli models materialize --verbose
```

#### `models query <model_name>`
Query a dbt staging model through DuckLake.

```bash
uv run python -m src.cli models query <model_name> [OPTIONS]
```

**Arguments**:
- `<model_name>`: Name of the dbt model to query (required)

**Options**:
- `--limit <number>`: Number of rows to display (default: 10)
- `--as-of-date <date>`: Query as of specific date (YYYY-MM-DD) for time travel
- `--show-schema`: Show model schema information
- `--verbose, -v`: Show detailed error information

**Examples**:
```bash
# Basic model query
uv run python -m src.cli models query stg_pbp

# Query with custom limit
uv run python -m src.cli models query stg_weekly --limit 25

# Show schema information
uv run python -m src.cli models query stg_team_desc --show-schema

# Time travel query
uv run python -m src.cli models query stg_pbp --as-of-date 2025-01-15 --limit 20

# Combine multiple options
uv run python -m src.cli models query stg_schedules --limit 15 --show-schema --verbose
```

#### `models catalog`
Show all tables registered in DuckLake catalog.

```bash
uv run python -m src.cli models catalog
```

**Output**: Rich-formatted table showing:
- Schema name
- Table name
- Number of versions
- Latest ETL date
- Total rows
- Created date

#### `models versions <table_name>`
Show version history for a specific table.

```bash
uv run python -m src.cli models versions <table_name>
```

**Arguments**:
- `<table_name>`: Table name in format 'schema.table' (required)

**Examples**:
```bash
# Show version history for pbp table
uv run python -m src.cli models versions nfl_raw.pbp

# Show history for team descriptions
uv run python -m src.cli models versions nfl_raw.team_desc
```

**Output**: Rich-formatted table showing:
- Version number
- ETL date
- Row count
- File size
- Created timestamp
- File path

#### `models sql <query>`
Run custom SQL query against DuckLake data.

```bash
uv run python -m src.cli models sql <query> [OPTIONS]
uv run python -m src.cli models sql --file <file_path>
```

**Arguments**:
- `<query>`: SQL query string (required if not using --file)

**Options**:
- `--file, -f <file_path>`: Read SQL query from file

**Examples**:
```bash
# Inline SQL query
uv run python -m src.cli models sql "SELECT COUNT(*) FROM 'data/team_desc/etl_date=*/data.parquet'"

# Team analysis query
uv run python -m src.cli models sql "
SELECT 
    team_division,
    COUNT(*) as team_count
FROM 'data/team_desc/etl_date=*/data.parquet'
WHERE team_division IS NOT NULL
GROUP BY team_division
ORDER BY team_count DESC
"

# Query from file
uv run python -m src.cli models sql --file queries/team_analysis.sql

# Complex analysis
uv run python -m src.cli models sql "
SELECT 
    season,
    COUNT(*) as games
FROM 'data/schedules/*/etl_date=*/data.parquet'
GROUP BY season
ORDER BY season
"
```

### 5. `api` - API Server Management

FastAPI server management and testing commands.

#### `api start`
Start the FastAPI server.

#### `api test`
Test API endpoints.

#### `api docs`
Generate API documentation.

#### `api validate`
Validate OpenAPI specification.

## Advanced Usage Patterns

### 1. Complete Data Workflow

```bash
# 1. Extract raw NFL data
uv run python -m src.cli extract dataset team_desc
uv run python -m src.cli extract dataset schedules --year 2024

# 2. Check extraction status
uv run python -m src.cli extract status

# 3. Materialize dbt models
uv run python -m src.cli models materialize --verbose

# 4. Explore transformed data
uv run python -m src.cli models catalog
uv run python -m src.cli models query stg_team_desc --show-schema

# 5. Perform analytics
uv run python -m src.cli models sql --file queries/team_analysis.sql
```

### 2. Time Travel Analysis

```bash
# Query current data
uv run python -m src.cli models query stg_pbp --limit 20

# Compare with historical data
uv run python -m src.cli models query stg_pbp --as-of-date 2025-01-01 --limit 20

# Check version history
uv run python -m src.cli models versions nfl_raw.pbp
```

### 3. Data Quality Monitoring

```bash
# Check data freshness
uv run python -m src.cli models sql "
SELECT 
    'team_desc' as dataset,
    COUNT(*) as row_count,
    MAX(dbt_loaded_at) as latest_load
FROM 'data/team_desc/etl_date=*/data.parquet'
WHERE dbt_loaded_at IS NOT NULL
"

# Validate data completeness
uv run python -m src.cli models query stg_team_desc --show-schema
```

## Error Handling

The CLI provides comprehensive error handling with different verbosity levels:

- **Basic errors**: Clear, user-friendly error messages
- **Verbose mode (`-v`)**: Detailed error information with stack traces
- **Validation errors**: Specific guidance on fixing command syntax or parameters

## Configuration

### Environment Variables

The CLI respects the following environment variables:

- `POSTGRES_HOST`: PostgreSQL host for DuckLake catalog (default: localhost)
- `POSTGRES_PORT`: PostgreSQL port (default: 5433)
- `POSTGRES_DATABASE`: Database name (default: nfl_ducklake)
- `POSTGRES_USER`: Database user (default: nfl_user)
- `POSTGRES_PASSWORD`: Database password (default: nfl_password)
- `DUCKDB_DATABASE_PATH`: DuckDB database path (default: data/nfl_analytics.duckdb)
- `NFL_DATA_PATH`: NFL data storage path (default: data/)

### Configuration Files

Dataset-specific configurations are stored in `configs/datasets/` with YAML files for each of the 19 supported NFL datasets.

## Supported NFL Datasets

The CLI supports all 19 datasets from `nfl_data_py`:

1. **pbp** - Play-by-play data (1999+)
2. **weekly** - Weekly player statistics (1999+)
3. **seasonal** - Seasonal player statistics (1999+)
4. **weekly_rosters** - Weekly team rosters (1999+)
5. **seasonal_rosters** - Seasonal team rosters (1999+)
6. **schedules** - Game schedules (1999+)
7. **team_desc** - Team descriptions and information (no year limit)
8. **officials** - Game officials (2001+)
9. **combine** - NFL Combine results (1987+)
10. **draft_picks** - NFL Draft picks (1936+)
11. **qbr** - Weekly QBR data (2006+)
12. **weekly_pfr** - Pro Football Reference weekly stats (1932+)
13. **seasonal_pfr** - Pro Football Reference seasonal stats (1932+)
14. **injuries** - Player injury reports (2009+)
15. **depth_charts** - Team depth charts (2001+)
16. **snap_counts** - Player snap counts (2012+)
17. **ftn_data** - Fantasy Points allowed data (2018+)
18. **ngs_data** - Next Gen Stats data (2016+)
19. **players** - Player information (no year limit)

## Best Practices

### 1. Data Extraction
- Use incremental extraction for regular updates
- Validate data after extraction with `--validate` flag
- Monitor extraction status regularly
- Clean up old files periodically

### 2. Model Management
- Materialize models after new data extraction
- Use time travel for historical analysis comparisons
- Check catalog versions before important analyses
- Save complex SQL queries to files for reuse

### 3. Performance
- Use `--limit` option for large datasets during exploration
- Leverage DuckDB's columnar performance for analytics
- Monitor file sizes and row counts through catalog

### 4. Troubleshooting
- Use `--verbose` flag for detailed error information
- Check `models catalog` to verify data availability
- Validate environment configuration before running commands
- Test with small datasets before large-scale operations

## Integration with Other Tools

### dbt Integration
- Models automatically discovered from `dbt/models/staging/`
- Seamless integration with existing dbt workflows
- Support for dbt metadata and documentation

### Dagster Integration  
- Direct asset materialization triggers
- Real-time progress monitoring
- Integration with existing Dagster schedules

### DuckLake Integration
- Time travel capabilities through PostgreSQL catalog
- Version-based data access
- ACID transaction support

This CLI reference provides comprehensive coverage of all available commands and options. For additional examples and workflows, see the main README.md and demo script at `scripts/demo_dbt_models.py`.