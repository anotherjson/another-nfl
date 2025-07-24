# NFL Analytics Platform - CLI Model Operations Troubleshooting Guide

## Overview

This guide provides solutions to common issues encountered when using the NFL Analytics Platform's CLI model operations and DuckLake integration.

## CLI Model Operations Issues

### 1. "models" Command Not Found

**Symptoms**:
```bash
$ uv run python -m src.cli models list
Error: No such command 'models'
```

**Causes & Solutions**:

1. **Outdated CLI version**: Ensure you have the latest version of the CLI with model operations support.
   ```bash
   # Check CLI help to see if models command exists
   uv run python -m src.cli --help
   ```

2. **Import error in DuckLakeManager**: Check if there are missing dependencies.
   ```bash
   # Test the import directly
   python -c "from src.ducklake_manager import DuckLakeManager"
   ```

3. **Missing dependencies**: Install required packages.
   ```bash
   uv sync --dev
   ```

### 2. PostgreSQL Connection Errors

**Symptoms**:
```bash
$ uv run python -m src.cli models catalog
Error: Failed to get catalog tables: connection to server at "localhost", port 5433 failed
```

**Causes & Solutions**:

1. **PostgreSQL not running**: Start the PostgreSQL service.
   ```bash
   # Start PostgreSQL for DuckLake catalog
   ./scripts/postgres_start.sh
   
   # Check if PostgreSQL is running
   ps aux | grep postgres
   ```

2. **Wrong connection parameters**: Check environment variables.
   ```bash
   # Check current settings
   echo $POSTGRES_HOST $POSTGRES_PORT $POSTGRES_DATABASE
   
   # Set correct values if needed
   export POSTGRES_HOST=localhost
   export POSTGRES_PORT=5433
   export POSTGRES_DATABASE=nfl_ducklake
   export POSTGRES_USER=nfl_user
   export POSTGRES_PASSWORD=nfl_password
   ```

3. **Database not initialized**: Initialize the DuckLake catalog.
   ```bash
   uv run python scripts/register_existing_data.py
   ```

### 3. DuckDB Extension Loading Issues

**Symptoms**:
```bash
$ uv run python -m src.cli models query stg_pbp
Error: Extension "parquet" not found
```

**Causes & Solutions**:

1. **DuckDB extensions not installed**: Install required extensions manually.
   ```python
   import duckdb
   conn = duckdb.connect("data/nfl_analytics.duckdb")
   conn.execute("INSTALL httpfs")
   conn.execute("INSTALL parquet")
   conn.execute("LOAD httpfs")
   conn.execute("LOAD parquet")
   conn.close()
   ```

2. **Permission issues**: Check write permissions to DuckDB database directory.
   ```bash
   ls -la data/
   chmod 755 data/
   ```

3. **Corrupted DuckDB file**: Remove and recreate the database.
   ```bash
   rm -f data/nfl_analytics.duckdb
   # Re-run model operations to recreate
   ```

### 4. Dagster Materialization Failures

**Symptoms**:
```bash
$ uv run python -m src.cli models materialize
Error: Failed to trigger materialization: dagster command not found
```

**Causes & Solutions**:

1. **Dagster not installed**: Ensure Dagster is available.
   ```bash
   # Check if dagster is installed
   uv run dagster --version
   
   # Install if missing
   uv add dagster-webserver
   ```

2. **Dagster not running**: Start Dagster development server.
   ```bash
   # Start Dagster in background
   uv run dagster dev -f nfl_dagster/definitions.py &
   ```

3. **Wrong asset name**: Check available Dagster assets.
   ```bash
   # List available assets
   uv run dagster asset list -f nfl_dagster/definitions.py
   ```

4. **dbt compilation errors**: Fix dbt models before materialization.
   ```bash
   # Test dbt compilation
   cd dbt && uv run dbt compile
   
   # Fix any SQL errors and retry
   ```

### 5. Time Travel Query Issues

**Symptoms**:
```bash
$ uv run python -m src.cli models query stg_pbp --as-of-date 2025-01-01
Error: No version found for nfl_raw.pbp as of 2025-01-01
```

**Causes & Solutions**:

1. **No data for that date**: Check available versions.
   ```bash
   # See what versions exist
   uv run python -m src.cli models versions nfl_raw.pbp
   
   # Use an available date from the output
   ```

2. **Date format issues**: Use correct YYYY-MM-DD format.
   ```bash
   # Correct format
   uv run python -m src.cli models query stg_pbp --as-of-date 2025-01-15
   
   # Wrong formats to avoid
   # --as-of-date "January 15, 2025"  # Wrong
   # --as-of-date "01/15/2025"        # Wrong
   ```

3. **Catalog not updated**: Re-register data with catalog.
   ```bash
   uv run python scripts/register_existing_data.py
   ```

### 6. Custom SQL Query Failures

**Symptoms**:
```bash
$ uv run python -m src.cli models sql "SELECT * FROM nonexistent_table"
Error: Table 'nonexistent_table' not found
```

**Causes & Solutions**:

1. **Wrong file paths**: Use correct Parquet file patterns.
   ```bash
   # Check available files
   find data/ -name "*.parquet" -type f
   
   # Use correct patterns
   uv run python -m src.cli models sql "SELECT * FROM 'data/team_desc/etl_date=*/data.parquet'"
   ```

2. **SQL syntax errors**: Validate SQL syntax.
   ```bash
   # Test simple query first
   uv run python -m src.cli models sql "SELECT COUNT(*) FROM 'data/team_desc/etl_date=*/data.parquet'"
   ```

3. **File permission issues**: Check read permissions.
   ```bash
   ls -la data/team_desc/etl_date=*/data.parquet
   chmod 644 data/team_desc/etl_date=*/data.parquet
   ```

### 7. Schema Display Issues

**Symptoms**:
```bash
$ uv run python -m src.cli models query stg_pbp --show-schema
Error: Failed to get schema for stg_pbp
```

**Causes & Solutions**:

1. **Model not materialized**: Ensure model exists in catalog.
   ```bash
   # Check if model is in catalog
   uv run python -m src.cli models catalog
   
   # Materialize if missing
   uv run python -m src.cli models materialize
   ```

2. **DuckDB connection issues**: Test DuckDB connectivity.
   ```python
   import duckdb
   conn = duckdb.connect("data/nfl_analytics.duckdb")
   print(conn.execute("SELECT 1").fetchall())
   conn.close()
   ```

## Performance Issues

### 8. Slow Query Performance

**Symptoms**: Queries taking excessive time.

**Causes & Solutions**:

1. **Large datasets**: Use LIMIT for testing.
   ```bash
   # Test with small dataset first
   uv run python -m src.cli models query stg_pbp --limit 100
   ```

2. **DuckDB memory settings**: Increase memory limits.
   ```python
   # In custom SQL
   SET memory_limit='8GB';
   SET threads TO 8;
   ```

3. **Index missing**: Consider partitioning strategies.
   ```bash
   # Check file sizes
   ls -lh data/*/etl_date=*/data.parquet
   ```

### 9. Memory Issues

**Symptoms**: Out of memory errors during large queries.

**Solutions**:
```bash
# Use smaller limits
uv run python -m src.cli models query stg_pbp --limit 1000

# Process data in chunks with custom SQL
uv run python -m src.cli models sql "SELECT * FROM 'data/pbp/*/etl_date=*/data.parquet' LIMIT 10000"
```

## Environment Issues

### 10. Permission Denied Errors

**Symptoms**:
```bash
Permission denied: '/path/to/data/file.parquet'
```

**Causes & Solutions**:

1. **File permissions**: Fix permissions.
   ```bash
   chmod -R 755 data/
   chmod -R 644 data/**/*.parquet
   ```

2. **Directory permissions**: Ensure directory accessibility.
   ```bash
   chmod 755 postgres/data/
   ```

### 11. Disk Space Issues

**Symptoms**: "No space left on device" errors.

**Causes & Solutions**:

1. **Clean up old data**: Use cleanup commands.
   ```bash
   # Clean old extractions
   uv run python -m src.cli extract cleanup --max-age-days 7
   
   # Remove old DuckDB files
   rm -f data/*.duckdb.wal data/*.duckdb.tmp
   ```

2. **Check disk usage**: Monitor space usage.
   ```bash
   du -sh data/
   df -h .
   ```

## Data Issues

### 12. No Data Available

**Symptoms**: Empty results from model queries.

**Causes & Solutions**:

1. **No data extracted yet**: Extract NFL data first.
   ```bash
   # Extract base data
   uv run python -m src.cli extract dataset team_desc
   uv run python -m src.cli extract dataset schedules --year 2024
   ```

2. **Data not registered**: Register existing data.
   ```bash
   uv run python scripts/register_existing_data.py
   ```

3. **Wrong file paths**: Check data directory structure.
   ```bash
   find data/ -name "*.parquet" -type f | head -10
   ```

### 13. Data Version Conflicts

**Symptoms**: Inconsistent results from same queries.

**Causes & Solutions**:

1. **Multiple ETL dates**: Check version consistency.
   ```bash
   uv run python -m src.cli models versions nfl_raw.team_desc
   ```

2. **Concurrent updates**: Ensure single process updates.
   ```bash
   # Check for running processes
   ps aux | grep python | grep cli
   ```

## Common Error Messages & Solutions

### "DuckLake catalog not initialized"
**Solution**: Run `uv run python scripts/register_existing_data.py`

### "dbt_staging_models asset not found"
**Solution**: Ensure Dagster is running and dbt models are defined properly

### "No module named psycopg2"
**Solution**: Run `uv add psycopg2-binary`

### "Extension loading failed"
**Solution**: Delete DuckDB file and recreate: `rm data/nfl_analytics.duckdb`

### "Time travel query returned no results"
**Solution**: Check available versions with `models versions` command

### "Rich formatting not working"
**Solution**: 
```bash
export FORCE_COLOR=1
uv add rich
```

### "Import error: DuckLakeManager"
**Solution**: Ensure you're in project root and dependencies are installed:
```bash
pwd  # Should show .../another-nfl
uv sync --dev
```

## Debugging Steps

### 1. Basic Environment Check
```bash
# Verify project structure
ls src/ducklake_manager.py  # Should exist

# Check dependencies
uv run python -c "import psycopg2, duckdb, pandas; print('Dependencies OK')"

# Test basic CLI
uv run python -m src.cli --help
```

### 2. PostgreSQL Status
```bash
# Check if PostgreSQL is running
ps aux | grep postgres

# Start if needed
./scripts/postgres_start.sh

# Test connection
uv run python -c "
import psycopg2
conn = psycopg2.connect(host='localhost', port=5433, database='nfl_ducklake', user='nfl_user', password='nfl_password')
print('PostgreSQL connection OK')
conn.close()
"
```

### 3. DuckDB Status
```bash
# Test DuckDB
uv run python -c "
import duckdb
conn = duckdb.connect('data/nfl_analytics.duckdb')
conn.execute('SELECT 1')
print('DuckDB connection OK')
conn.close()
"
```

### 4. Data Availability
```bash
# Check for data files
find data/ -name "*.parquet" -type f | wc -l

# List available datasets
ls -la data/
```

### 5. Comprehensive Test
```bash
# Run demo script to test full workflow
uv run python scripts/demo_dbt_models.py

# Run specific tests
uv run pytest tests/test_ducklake_manager.py -v
uv run pytest tests/test_cli_models.py -v
```

## Getting Additional Help

### Debug Mode
Always use verbose mode for detailed error information:
```bash
uv run python -m src.cli models query stg_pbp --verbose
```

### Log Files
Check logs for additional context:
```bash
# Check Dagster logs (if available)
ls -la logs/
tail -f logs/dagster.log

# Check PostgreSQL logs  
tail -f postgres/logs/postgresql.log
```

### Test Individual Components
```bash
# Test DuckLake manager
uv run python -c "
from src.ducklake_manager import DuckLakeManager
dm = DuckLakeManager()
print('Available models:', [m['name'] for m in dm.list_available_models()])
"

# Test CLI model listing
uv run python -m src.cli models list
```

For additional support beyond this troubleshooting guide, refer to:
- `README.md` - Main documentation
- `CLI_REFERENCE.md` - Comprehensive CLI reference
- `CLAUDE.md` - Development guide
- `ARCHITECTURE.md` - System architecture details