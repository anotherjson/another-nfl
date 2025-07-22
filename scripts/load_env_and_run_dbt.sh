#!/bin/bash
# Load environment variables from .env file and run dbt commands

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"

# Check if .env file exists
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ .env file not found at $ENV_FILE"
    exit 1
fi

# Load environment variables from .env file
echo "📁 Loading environment variables from .env file..."
set -o allexport
source "$ENV_FILE"
set +o allexport

# Change to dbt directory
cd "$PROJECT_ROOT/dbt"

# Run dbt command with loaded environment variables
echo "🏗️  Running dbt command: $@"
uv run dbt "$@"