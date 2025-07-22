# dbt Transform Container
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv package manager
RUN pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install Python dependencies with dbt
RUN uv sync --frozen --no-dev

# Copy dbt project
COPY dbt/ ./dbt/

# Create profiles directory
RUN mkdir -p /root/.dbt

# Copy dbt profiles
COPY docker/dbt-runner/profiles.yml /root/.dbt/profiles.yml

# Create non-root user
RUN useradd --create-home --shell /bin/bash dbt-user && \
    chown -R dbt-user:dbt-user /app
USER dbt-user

# Set dbt working directory
WORKDIR /app/dbt

# Health check
HEALTHCHECK --interval=60s --timeout=30s --start-period=10s --retries=3 \
    CMD uv run dbt debug || exit 1

# Default command - compile and test
CMD ["uv", "run", "dbt", "run"]