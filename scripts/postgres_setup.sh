#!/bin/bash
# Setup PostgreSQL database and user for NFL DuckLake project

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Setting up PostgreSQL database for NFL DuckLake..."

# Start PostgreSQL if not running
"$SCRIPT_DIR/postgres_start.sh"
sleep 2

# Create database and user
psql -h localhost -p 5433 -d postgres -c "
CREATE DATABASE nfl_ducklake;
CREATE USER nfl_user WITH PASSWORD 'nfl_password';
GRANT ALL PRIVILEGES ON DATABASE nfl_ducklake TO nfl_user;
ALTER USER nfl_user CREATEDB;
"

if [ $? -eq 0 ]; then
    echo "Database setup completed successfully"
    echo "Database: nfl_ducklake"
    echo "User: nfl_user"
    echo "Connection: postgresql://nfl_user:nfl_password@localhost:5433/nfl_ducklake"
else
    echo "Failed to setup database"
    exit 1
fi