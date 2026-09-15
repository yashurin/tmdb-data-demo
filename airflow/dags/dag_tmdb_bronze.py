"""Bronze extract: TMDB API v3 → MinIO JSON + Postgres jsonb catalog.

Raw payloads are stored unmodified. This DAG does not clean or conform data.

Trigger manually after `docker compose up`. Schedule is daily but catchup is off.
On success it emits Dataset `warehouse://bronze`, which schedules silver.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import project_path  # noqa: F401
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context

from bronze_catalog import catalog_run
from s3_landing import ensure_buckets as create_landing_buckets
from s3_landing import put_json, put_run_manifest, safe_run_id
from tmdb_client import get_client

log = logging.getLogger(__name__)

BRONZE_READY = Dataset("warehouse://bronze")


def _run_meta() -> tuple[str, str]:
    ctx = get_current_context()
    logical = ctx["logical_date"]
    run_id = safe_run_id(ctx["dag_run"].run_id)
    return logical.strftime("%Y-%m-%d"), run_id


def _int_var(name: str, env: str, default: int) -> int:
    raw = os.environ.get(env)
    try:
        return int(Variable.get(name, default_var=raw or default))
    except Exception:
        return int(raw or default)


@dag(
    dag_id="dag_tmdb_bronze",
    description="Extract TMDB resources to MinIO bronze and catalog jsonb in Postgres",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "tmdb-demo",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["tmdb", "bronze", "medallion"],
    doc_md=__doc__,
)
def dag_tmdb_bronze():
    @task(task_id="ensure_buckets")
    def ensure_buckets() -> str:
        created = create_landing_buckets()
        log.info("ensure_buckets created=%s", created)
        return "ok"

    @task
    def extract_ref_genres(_ready: str) -> int:
        ingest_date, run_id = _run_meta()
        client = get_client()
        payload = client.get("/genre/movie/list")
        genres = payload.get("genres") or []
        put_json(
            resource="genre",
            ingest_date=ingest_date,
            run_id=run_id,
            filename="genre_movie_list",
            payload=payload,
            request_path="/genre/movie/list",
            record_count=len(genres),
        )
        log.info("genre records=%s", len(genres))
        return len(genres)

    @task
    def extract_popular_movies(_ready: str) -> list[int]:
        ingest_date, run_id = _run_meta()
        pages = _int_var("tmdb_pages", "TMDB_PAGES", 5)
        client = get_client()
        ids: list[int] = []
        for payload in client.iter_pages("/movie/popular", max_pages=pages):
            page = int(payload.get("page") or 0)
            results = payload.get("results") or []
            put_json(
                resource="movie_popular",
                ingest_date=ingest_date,
                run_id=run_id,
                filename=f"page_{page:03d}",
                payload=payload,
                request_path="/movie/popular",
                record_count=len(results),
            )
            ids.extend(int(r["id"]) for r in results if r.get("id") is not None)
            log.info("popular page=%s records=%s", page, len(results))
        unique = sorted(set(ids))
        log.info("popular unique_movie_ids=%s from_pages=%s", len(unique), pages)
        return unique

    @task
    def extract_top_rated_movies(_ready: str) -> list[int]:
        ingest_date, run_id = _run_meta()
        pages = _int_var("tmdb_pages", "TMDB_PAGES", 5)
        client = get_client()
        ids: list[int] = []
        for payload in client.iter_pages("/movie/top_rated", max_pages=pages):
            page = int(payload.get("page") or 0)
            results = payload.get("results") or []
            put_json(
                resource="movie_top_rated",
                ingest_date=ingest_date,
                run_id=run_id,
                filename=f"page_{page:03d}",
                payload=payload,
                request_path="/movie/top_rated",
                record_count=len(results),
            )
            ids.extend(int(r["id"]) for r in results if r.get("id") is not None)
            log.info("top_rated page=%s records=%s", page, len(results))
        unique = sorted(set(ids))
        log.info("top_rated unique_movie_ids=%s from_pages=%s", len(unique), pages)
        return unique

    @task
    def extract_movie_enrichment(popular_ids: list[int], top_rated_ids: list[int]) -> list[int]:
        ingest_date, run_id = _run_meta()
        movie_ids = sorted(set(popular_ids) | set(top_rated_ids))
        client = get_client()
        person_ids: set[int] = set()
        log.info("enriching movies=%s", len(movie_ids))
        for movie_id in movie_ids:
            details = client.get(f"/movie/{movie_id}")
            put_json(
                resource="movie",
                ingest_date=ingest_date,
                run_id=run_id,
                filename=str(movie_id),
                payload=details,
                request_path=f"/movie/{movie_id}",
            )
            credits = client.get(f"/movie/{movie_id}/credits")
            put_json(
                resource="credits",
                ingest_date=ingest_date,
                run_id=run_id,
                filename=str(movie_id),
                payload=credits,
                request_path=f"/movie/{movie_id}/credits",
            )
            keywords = client.get(f"/movie/{movie_id}/keywords")
            put_json(
                resource="keywords",
                ingest_date=ingest_date,
                run_id=run_id,
                filename=str(movie_id),
                payload=keywords,
                request_path=f"/movie/{movie_id}/keywords",
            )
            for row in (credits.get("cast") or []) + (credits.get("crew") or []):
                if row.get("id") is not None:
                    person_ids.add(int(row["id"]))
        log.info("movie details/credits/keywords landed=%s people_seen=%s", len(movie_ids), len(person_ids))
        return sorted(person_ids)

    @task
    def extract_people(person_ids: list[int]) -> int:
        ingest_date, run_id = _run_meta()
        cap = _int_var("tmdb_max_people", "TMDB_MAX_PEOPLE", 200)
        chosen = person_ids[:cap]
        client = get_client()
        log.info("person extract cap=%s of unique=%s", len(chosen), len(person_ids))
        for person_id in chosen:
            payload = client.get(f"/person/{person_id}")
            put_json(
                resource="person",
                ingest_date=ingest_date,
                run_id=run_id,
                filename=str(person_id),
                payload=payload,
                request_path=f"/person/{person_id}",
            )
        return len(chosen)

    @task
    def catalog_bronze(_genres: int, _people: int) -> dict:
        ingest_date, run_id = _run_meta()
        counts = catalog_run(ingest_date=ingest_date, run_id=run_id)
        log.info("bronze catalog counts=%s", counts)
        return counts

    @task(outlets=[BRONZE_READY])
    def bronze_run_manifest(catalog_counts: dict) -> dict:
        ingest_date, run_id = _run_meta()
        counts = {k: int(v) for k, v in catalog_counts.items()}
        put_run_manifest(ingest_date=ingest_date, run_id=run_id, counts=counts)
        log.info("bronze complete ingest_date=%s run_id=%s", ingest_date, run_id)
        return counts

    ready = ensure_buckets()
    genres = extract_ref_genres(ready)
    popular = extract_popular_movies(ready)
    top_rated = extract_top_rated_movies(ready)
    people = extract_people(extract_movie_enrichment(popular, top_rated))
    cataloged = catalog_bronze(genres, people)
    bronze_run_manifest(cataloged)


dag_tmdb_bronze()
