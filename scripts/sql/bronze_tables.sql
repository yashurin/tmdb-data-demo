-- Bronze registry (also created by the catalog task via CREATE TABLE IF NOT EXISTS).
-- Idempotent. Safe to re-run.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.ingest_objects (
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    resource TEXT NOT NULL,
    ingest_date DATE NOT NULL,
    dag_run_id TEXT NOT NULL,
    bytes BIGINT,
    etag TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bucket, key)
);

CREATE TABLE IF NOT EXISTS bronze.raw_payloads (
    resource TEXT NOT NULL,
    tmdb_id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    source_key TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL,
    dag_run_id TEXT NOT NULL,
    PRIMARY KEY (resource, tmdb_id, source_key)
);
