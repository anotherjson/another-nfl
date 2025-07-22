# Streamlit Analytics App Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
COPY visualizations/ ./visualizations/

# Install uv package manager
RUN pip install uv

# Install dependencies
RUN uv sync --no-dev

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash streamlit
RUN chown -R streamlit:streamlit /app
USER streamlit

# Expose Streamlit port
EXPOSE 8501

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Run Streamlit app
CMD ["uv", "run", "streamlit", "run", "visualizations/streamlit_app/main.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]