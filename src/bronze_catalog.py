"""Register MinIO bronze objects in Postgres warehouse.bronze (jsonb as-is)."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from s3_landing import (
    BRONZE_BUCKET,
    get_json,
    is_manifest_key,
    list_run_objects,
    parse_bronze_key,
)

log = logging.getLogger(__name__)

DDL_INGEST_OBJECTS = """
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
"""

DDL_RAW_PAYLOADS = """
CREATE TABLE IF NOT EXISTS bronze.raw_payloads (
    resource TEXT NOT NULL,
    tmdb_id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    source_key TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL,
    dag_run_id TEXT NOT NULL,
    PRIMARY KEY (resource, tmdb_id, source_key)
);
"""

PAYLOAD_RESOURCES = {
    "genre",
    "movie_popular",
    "movie_top_rated",
    "movie",
    "credits",
    "keywords",
    "person",
}


def warehouse_conn():
    import psycopg2

    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_HOST", "postgres"),
        port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
        dbname=os.environ.get("WAREHOUSE_DB", "warehouse"),
        user=os.environ.get("POSTGRES_USER", "tmdb"),
        password=os.environ.get("POSTGRES_PASSWORD", "tmdb"),
    )


def ensure_tables(conn=None) -> None:
    own = conn is None
    conn = conn or warehouse_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            cur.execute(DDL_INGEST_OBJECTS)
            cur.execute(DDL_RAW_PAYLOADS)
        conn.commit()
    finally:
        if own:
            conn.close()


def tmdb_id_from_payload(resource: str, payload: dict[str, Any], filename: str) -> int:
    if resource in {"movie", "credits", "keywords", "person"}:
        if payload.get("id") is not None:
            return int(payload["id"])
        stem = filename.replace(".json", "")
        return int(stem)
    if resource in {"movie_popular", "movie_top_rated"}:
        return int(payload.get("page") or 0)
    if resource == "genre":
        return 0
    return 0


def catalog_run(ingest_date: str, run_id: str, bucket: str | None = None) -> dict[str, int]:
    """List this run's bronze objects, upsert ingest_objects + raw_payloads."""
    from psycopg2.extras import Json, execute_values

    bucket = bucket or BRONZE_BUCKET
    objects = list_run_objects(ingest_date=ingest_date, run_id=run_id, bucket=bucket)
    ensure_tables()

    ingest_rows = []
    payload_rows = []
    now = datetime.now(timezone.utc)
    skipped_manifests = 0

    for obj in objects:
        key = obj["key"]
        if is_manifest_key(key):
            skipped_manifests += 1
            continue
        meta = parse_bronze_key(key)
        resource = meta["resource"]
        ingest_rows.append(
            (
                bucket,
                key,
                resource,
                meta["ingest_date"],
                run_id,
                obj.get("bytes"),
                obj.get("etag"),
                now,
            )
        )
        if resource not in PAYLOAD_RESOURCES:
            continue
        payload = get_json(key, bucket=bucket)
        if not isinstance(payload, dict):
            log.warning("skip non-object json %s", key)
            continue
        tmdb_id = tmdb_id_from_payload(resource, payload, meta["filename"])
        payload_rows.append((resource, tmdb_id, Json(payload), key, now, run_id))

    conn = warehouse_conn()
    try:
        with conn.cursor() as cur:
            if ingest_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO bronze.ingest_objects
                        (bucket, key, resource, ingest_date, dag_run_id, bytes, etag, loaded_at)
                    VALUES %s
                    ON CONFLICT (bucket, key) DO UPDATE SET
                        resource = EXCLUDED.resource,
                        ingest_date = EXCLUDED.ingest_date,
                        dag_run_id = EXCLUDED.dag_run_id,
                        bytes = EXCLUDED.bytes,
                        etag = EXCLUDED.etag,
                        loaded_at = EXCLUDED.loaded_at
                    """,
                    ingest_rows,
                )
            if payload_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO bronze.raw_payloads
                        (resource, tmdb_id, payload, source_key, extracted_at, dag_run_id)
                    VALUES %s
                    ON CONFLICT (resource, tmdb_id, source_key) DO UPDATE SET
                        payload = EXCLUDED.payload,
                        extracted_at = EXCLUDED.extracted_at,
                        dag_run_id = EXCLUDED.dag_run_id
                    """,
                    payload_rows,
                )
        conn.commit()
    finally:
        conn.close()

    counts = {
        "ingest_objects": len(ingest_rows),
        "raw_payloads": len(payload_rows),
        "skipped_manifests": skipped_manifests,
        "listed": len(objects),
    }
    log.info("cataloged bronze run %s %s", run_id, counts)
    return counts
