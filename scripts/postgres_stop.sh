#!/bin/bash
# Stop local PostgreSQL instance for NFL DuckLake project

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PG_DATA="$PROJECT_ROOT/postgres/data"

echo "Stopping PostgreSQL for NFL DuckLake project..."

pg_ctl -D "$PG_DATA" stop

if [ $? -eq 0 ]; then
    echo "PostgreSQL stopped successfully"
else
    echo "Failed to stop PostgreSQL or it was not running"
    exit 1
fi