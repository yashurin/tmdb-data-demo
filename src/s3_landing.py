"""MinIO/S3 bronze landing helpers. Key layout is a pure function (unit-tested)."""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "tmdb-bronze")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "tmdb-silver")

_UNSAFE_RUN_ID = re.compile(r"[^A-Za-z0-9._-]+")


def safe_run_id(run_id: str) -> str:
    return _UNSAFE_RUN_ID.sub("-", run_id)[:200]


def build_bronze_key(resource: str, ingest_date: str, run_id: str, filename: str) -> str:
    """s3://tmdb-bronze/tmdb/{resource}/ingest_date=YYYY-MM-DD/run_id={id}/{filename}.json"""
    name = filename if filename.endswith(".json") else f"{filename}.json"
    return f"tmdb/{resource}/ingest_date={ingest_date}/run_id={safe_run_id(run_id)}/{name}"


def parse_bronze_key(key: str) -> dict[str, str]:
    """Parse a bronze object key into resource / ingest_date / run_id / filename."""
    parts = key.split("/")
    if len(parts) < 5 or parts[0] != "tmdb":
        raise ValueError(f"Not a bronze key: {key}")
    resource = parts[1]
    date_part = parts[2]
    run_part = parts[3]
    filename = "/".join(parts[4:])
    if not date_part.startswith("ingest_date=") or not run_part.startswith("run_id="):
        raise ValueError(f"Not a bronze key: {key}")
    return {
        "resource": resource,
        "ingest_date": date_part.split("=", 1)[1],
        "run_id": run_part.split("=", 1)[1],
        "filename": filename,
    }


def is_manifest_key(key: str) -> bool:
    return key.endswith(".manifest.json") or key.endswith("/manifest.json")


def minio_client():
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def ensure_buckets(client=None, buckets: tuple[str, ...] | None = None) -> list[str]:
    client = client or minio_client()
    wanted = list(buckets or (BRONZE_BUCKET, SILVER_BUCKET))
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    created = []
    for name in wanted:
        if name not in existing:
            client.create_bucket(Bucket=name)
            created.append(name)
            log.info("created bucket %s", name)
        else:
            log.info("bucket exists %s", name)
    return created


def put_json(
    *,
    resource: str,
    ingest_date: str,
    run_id: str,
    filename: str,
    payload: Any,
    request_path: str,
    http_status: int = 200,
    record_count: int | None = None,
    bucket: str | None = None,
    client=None,
) -> str:
    """Write unmodified JSON plus a sidecar manifest. Returns the data object key."""
    client = client or minio_client()
    bucket = bucket or BRONZE_BUCKET
    key = build_bronze_key(resource, ingest_date, run_id, filename)
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")

    if record_count is None:
        record_count = _guess_record_count(resource, payload)

    extracted_at = datetime.now(timezone.utc).isoformat()
    manifest = {
        "request_path": request_path,
        "http_status": http_status,
        "extracted_at": extracted_at,
        "record_count": record_count,
        "resource": resource,
        "data_key": key,
        "bytes": len(body),
    }
    manifest_name = filename[: -5] if filename.endswith(".json") else filename
    manifest_key = build_bronze_key(resource, ingest_date, run_id, f"{manifest_name}.manifest")
    client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    log.info("landed s3://%s/%s records=%s bytes=%s", bucket, key, record_count, len(body))
    return key


def put_run_manifest(
    *,
    ingest_date: str,
    run_id: str,
    counts: dict[str, int],
    extra: dict[str, Any] | None = None,
    bucket: str | None = None,
    client=None,
) -> str:
    client = client or minio_client()
    bucket = bucket or BRONZE_BUCKET
    payload = {
        "ingest_date": ingest_date,
        "run_id": safe_run_id(run_id),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "counts": counts,
        **(extra or {}),
    }
    key = build_bronze_key("_run", ingest_date, run_id, "manifest")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    log.info("run manifest s3://%s/%s counts=%s", bucket, key, counts)
    return key


def list_run_objects(*, ingest_date: str, run_id: str, bucket: str | None = None, client=None) -> list[dict[str, Any]]:
    client = client or minio_client()
    bucket = bucket or BRONZE_BUCKET
    needle = f"/run_id={safe_run_id(run_id)}/"
    date_needle = f"ingest_date={ingest_date}"
    objects: list[dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="tmdb/"):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if needle in key and date_needle in key:
                objects.append(
                    {
                        "key": key,
                        "bytes": obj.get("Size"),
                        "etag": (obj.get("ETag") or "").strip('"'),
                        "last_modified": obj.get("LastModified"),
                    }
                )
    return objects


def get_json(key: str, bucket: str | None = None, client=None) -> Any:
    client = client or minio_client()
    bucket = bucket or BRONZE_BUCKET
    response = client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def bucket_exists(name: str, client=None) -> bool:
    from botocore.exceptions import ClientError

    client = client or minio_client()
    try:
        client.head_bucket(Bucket=name)
        return True
    except ClientError:
        return False


def _guess_record_count(resource: str, payload: Any) -> int:
    if not isinstance(payload, dict):
        return 1
    if resource == "genre":
        return len(payload.get("genres") or [])
    if resource in {"movie_popular", "movie_top_rated"}:
        return len(payload.get("results") or [])
    if resource == "credits":
        return len(payload.get("cast") or []) + len(payload.get("crew") or [])
    if resource == "keywords":
        return len(payload.get("keywords") or [])
    return 1
