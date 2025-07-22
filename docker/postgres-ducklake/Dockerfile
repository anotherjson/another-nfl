# PostgreSQL + DuckDB Container for DuckLake Catalog
FROM postgres:15-alpine

# Install system dependencies
RUN apk add --no-cache \
    python3 \
    py3-pip \
    curl \
    git

# Install DuckDB CLI
RUN apk add --no-cache --repository=http://dl-cdn.alpinelinux.org/alpine/edge/testing duckdb

# Install Python packages for DuckLake integration
RUN pip3 install --no-cache-dir \
    psycopg2-binary \
    duckdb \
    pandas \
    pyarrow

# Create directories for scripts and data
RUN mkdir -p /opt/nfl-scripts /opt/nfl-data

# Copy PostgreSQL configuration
COPY docker/postgres-ducklake/postgresql.conf /etc/postgresql/postgresql.conf
COPY docker/postgres-ducklake/pg_hba.conf /etc/postgresql/pg_hba.conf

# Copy initialization scripts
COPY docker/postgres-ducklake/init-db.sh /docker-entrypoint-initdb.d/01-init-db.sh
COPY scripts/register_existing_data.py /opt/nfl-scripts/
COPY scripts/test_ducklake_integration.py /opt/nfl-scripts/

# Set permissions
RUN chmod +x /docker-entrypoint-initdb.d/01-init-db.sh && \
    chown -R postgres:postgres /opt/nfl-scripts /opt/nfl-data

# Expose PostgreSQL port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=60s --retries=3 \
    CMD pg_isready -U $POSTGRES_USER -d $POSTGRES_DB || exit 1

# Use the default PostgreSQL entrypoint
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]