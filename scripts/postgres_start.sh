#!/bin/bash
# Start local PostgreSQL instance for NFL DuckLake project

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PG_DATA="$PROJECT_ROOT/postgres/data"
PG_LOG="$PROJECT_ROOT/postgres/logs/postgresql.log"

echo "Starting PostgreSQL for NFL DuckLake project..."
echo "Data directory: $PG_DATA"
echo "Log file: $PG_LOG"

# Create logs directory if it doesn't exist
mkdir -p "$PROJECT_ROOT/postgres/logs"

# Start PostgreSQL
pg_ctl -D "$PG_DATA" -l "$PG_LOG" start

if [ $? -eq 0 ]; then
    echo "PostgreSQL started successfully on port 5433"
    echo "Connection string: postgresql://localhost:5433/nfl_ducklake"
else
    echo "Failed to start PostgreSQL"
    exit 1
fi