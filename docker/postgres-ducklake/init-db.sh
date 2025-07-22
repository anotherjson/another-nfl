#!/bin/bash
# PostgreSQL initialization script for NFL DuckLake catalog

set -e

# Create NFL-specific databases and users
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create NFL catalog database
    CREATE DATABASE nfl_catalog;
    
    -- Create NFL data user
    CREATE USER nfl_user WITH PASSWORD 'nfl_secure_password_change_in_production';
    
    -- Grant permissions
    GRANT ALL PRIVILEGES ON DATABASE nfl_catalog TO nfl_user;
    GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_DB TO nfl_user;
    
    -- Connect to NFL catalog and create initial schema
    \c nfl_catalog;
    
    -- Create schemas
    CREATE SCHEMA IF NOT EXISTS ducklake_catalog;
    CREATE SCHEMA IF NOT EXISTS nfl_metadata;
    
    -- Grant schema permissions
    GRANT ALL ON SCHEMA ducklake_catalog TO nfl_user;
    GRANT ALL ON SCHEMA nfl_metadata TO nfl_user;
    
    -- Create DuckLake catalog tables
    CREATE TABLE IF NOT EXISTS ducklake_catalog.tables (
        id SERIAL PRIMARY KEY,
        namespace_name VARCHAR(255) NOT NULL,
        table_name VARCHAR(255) NOT NULL,
        table_type VARCHAR(50) NOT NULL DEFAULT 'TABLE',
        location TEXT,
        current_snapshot_id BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(namespace_name, table_name)
    );
    
    CREATE TABLE IF NOT EXISTS ducklake_catalog.snapshots (
        id SERIAL PRIMARY KEY,
        table_id INTEGER REFERENCES ducklake_catalog.tables(id),
        snapshot_id BIGINT NOT NULL,
        parent_snapshot_id BIGINT,
        operation VARCHAR(50) NOT NULL,
        summary JSONB,
        schema_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS ducklake_catalog.schema_versions (
        id SERIAL PRIMARY KEY,
        schema_id INTEGER UNIQUE NOT NULL,
        schema_json JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create NFL metadata tables
    CREATE TABLE IF NOT EXISTS nfl_metadata.extraction_log (
        id SERIAL PRIMARY KEY,
        dataset_name VARCHAR(100) NOT NULL,
        extraction_date DATE NOT NULL,
        year_extracted INTEGER,
        records_count INTEGER,
        file_size_bytes BIGINT,
        extraction_duration_seconds INTEGER,
        status VARCHAR(20) DEFAULT 'SUCCESS',
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS nfl_metadata.data_quality_checks (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(255) NOT NULL,
        check_name VARCHAR(255) NOT NULL,
        check_type VARCHAR(50) NOT NULL,
        status VARCHAR(20) NOT NULL,
        result_details JSONB,
        checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create indexes for performance
    CREATE INDEX IF NOT EXISTS idx_tables_namespace_name ON ducklake_catalog.tables(namespace_name);
    CREATE INDEX IF NOT EXISTS idx_snapshots_table_id ON ducklake_catalog.snapshots(table_id);
    CREATE INDEX IF NOT EXISTS idx_extraction_log_dataset ON nfl_metadata.extraction_log(dataset_name);
    CREATE INDEX IF NOT EXISTS idx_extraction_log_date ON nfl_metadata.extraction_log(extraction_date);
    
    -- Grant table permissions
    GRANT ALL ON ALL TABLES IN SCHEMA ducklake_catalog TO nfl_user;
    GRANT ALL ON ALL TABLES IN SCHEMA nfl_metadata TO nfl_user;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA ducklake_catalog TO nfl_user;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA nfl_metadata TO nfl_user;
    
EOSQL

echo "NFL PostgreSQL catalog database initialized successfully"