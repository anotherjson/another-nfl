# Dagster Server Container
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install uv package manager
RUN pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install Python dependencies
RUN uv sync --frozen --no-dev

# Copy Dagster project
COPY nfl_dagster/ ./nfl_dagster/
COPY src/ ./src/
COPY dbt/ ./dbt/

# Create dagster home directory
RUN mkdir -p /opt/dagster/dagster_home

# Copy Dagster configuration
COPY docker/dagster-server/dagster.yaml /opt/dagster/dagster_home/dagster.yaml

# Create non-root user
RUN useradd --create-home --shell /bin/bash dagster-user && \
    chown -R dagster-user:dagster-user /app /opt/dagster
USER dagster-user

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/app

# Expose Dagster webserver port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:3000/server_info || exit 1

# Default command
CMD ["uv", "run", "dagster", "dev", "-f", "nfl_dagster/definitions.py", "-h", "0.0.0.0", "-p", "3000"]