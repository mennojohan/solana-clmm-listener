-- Database initialization script for Solana CLMM Analyzer
-- This script runs automatically when the container starts

-- Ensure the database exists (created by environment variables)
-- Create the user if not exists (created by environment variables)

-- Set up basic extensions for TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create a basic schema for future tables
CREATE SCHEMA IF NOT EXISTS clmm;

-- Set search path to include our schema
ALTER DATABASE solana_clmm SET search_path TO clmm,public;

-- Grant permissions to our user
GRANT ALL PRIVILEGES ON SCHEMA clmm TO clmm_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA clmm TO clmm_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA clmm TO clmm_user;

-- Create a basic table structure for future use
-- This will be expanded as we develop the prototype
CREATE TABLE IF NOT EXISTS clmm.metadata (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Insert basic metadata about the database
INSERT INTO clmm.metadata (key, value) VALUES
    ('database_version', '1.0'),
    ('initialized_at', NOW()::TEXT)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();

-- Create a hypertable for time-series data (will be used later)
-- This is a placeholder for future time-series data
CREATE TABLE IF NOT EXISTS clmm.time_series_data (
    time TIMESTAMPTZ NOT NULL,
    metric_name TEXT NOT NULL,
    value DOUBLE PRECISION,
    tags JSONB
);

SELECT create_hypertable('clmm.time_series_data', 'time', if_not_exists => TRUE);