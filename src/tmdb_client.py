"""TMDB REST API v3 client. Never logs the API key."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Iterator
from urllib.parse import urljoin

import requests

log = logging.getLogger(__name__)

TMDB_BASE_URL = "https://api.themoviedb.org/3"
DEFAULT_TIMEOUT = 30
MAX_ATTEMPTS = 6
DEFAULT_MIN_INTERVAL_SEC = 0.26  # stay under ~40 req / 10s


class TmdbError(RuntimeError):
    pass


class TmdbClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = TMDB_BASE_URL,
        min_interval_sec: float = DEFAULT_MIN_INTERVAL_SEC,
        session: requests.Session | None = None,
    ) -> None:
        if not api_key or api_key == "replace-me":
            raise TmdbError("TMDB_API_KEY is missing or still set to replace-me")
        self._api_key = api_key
        self.base_url = base_url.rstrip("/") + "/"
        self.min_interval_sec = min_interval_sec
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "tmdb-medallion-elt/1.0")
        self._last_request_at = 0.0

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = urljoin(self.base_url, path.lstrip("/"))
        query = dict(params or {})
        query["api_key"] = self._api_key
        last_error: Exception | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._throttle()
            log.info("GET %s params=%s attempt=%s", path, _redact(query), attempt)
            try:
                response = self.session.get(url, params=query, timeout=DEFAULT_TIMEOUT)
            except requests.RequestException as exc:
                last_error = exc
                sleep_s = min(2 ** attempt, 30)
                log.warning("TMDB request error on %s: %s; retry in %ss", path, exc, sleep_s)
                time.sleep(sleep_s)
                continue

            if response.status_code == 429 or response.status_code >= 500:
                retry_after = response.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else min(2 ** attempt, 30)
                log.warning(
                    "TMDB %s on %s; retry in %ss",
                    response.status_code,
                    path,
                    sleep_s,
                )
                time.sleep(sleep_s)
                last_error = TmdbError(f"HTTP {response.status_code} for {path}")
                continue

            if response.status_code >= 400:
                raise TmdbError(f"HTTP {response.status_code} for {path}: {response.text[:300]}")

            self._last_request_at = time.monotonic()
            payload = response.json()
            if not isinstance(payload, dict):
                raise TmdbError(f"Unexpected TMDB payload type {type(payload)} for {path}")
            return payload

        raise TmdbError(f"Exhausted retries for {path}: {last_error}")

    def iter_pages(
        self,
        path: str,
        max_pages: int,
        extra_params: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        page = 1
        total_pages = 1
        while page <= max_pages and page <= total_pages:
            payload = self.get(path, {"page": page, **(extra_params or {})})
            total_pages = int(payload.get("total_pages") or 1)
            yield payload
            page += 1

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.min_interval_sec:
            time.sleep(self.min_interval_sec - elapsed)


def get_api_key() -> str:
    key = (os.environ.get("TMDB_API_KEY") or "").strip()
    if key and key != "replace-me":
        return key
    try:
        from airflow.models import Variable

        key = (Variable.get("tmdb_api_key", default_var="") or "").strip()
    except Exception:
        key = ""
    if not key or key == "replace-me":
        raise TmdbError(
            "TMDB_API_KEY is not set. Copy .env.example to .env and set a real key "
            "(or Airflow Variable tmdb_api_key)."
        )
    return key


def get_client() -> TmdbClient:
    return TmdbClient(api_key=get_api_key())


def _redact(params: dict[str, Any]) -> dict[str, Any]:
    return {k: ("***" if k.lower() in {"api_key", "authorization"} else v) for k, v in params.items()}
