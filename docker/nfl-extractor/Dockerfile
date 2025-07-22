# NFL Data Extractor Container
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv package manager
RUN pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install Python dependencies
RUN uv sync --frozen --no-dev

# Copy source code
COPY src/ ./src/
COPY configs/ ./configs/

# Create data directory
RUN mkdir -p /data

# Create non-root user
RUN useradd --create-home --shell /bin/bash nfl-user && \
    chown -R nfl-user:nfl-user /app /data
USER nfl-user

# Expose port for health checks
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command
CMD ["uv", "run", "python", "-m", "src.cli", "extract", "status"]