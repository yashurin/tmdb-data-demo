from s3_landing import build_bronze_key, is_manifest_key, parse_bronze_key, safe_run_id


def test_build_bronze_key_appends_json() -> None:
    key = build_bronze_key("movie", "2026-09-15", "manual__abc", "550")
    assert key == "tmdb/movie/ingest_date=2026-09-15/run_id=manual__abc/550.json"


def test_build_bronze_key_keeps_json_suffix() -> None:
    key = build_bronze_key("genre", "2026-09-15", "run1", "genre_movie_list.json")
    assert key.endswith("/genre_movie_list.json")


def test_safe_run_id_strips_colons() -> None:
    assert ":" not in safe_run_id("manual__2026-09-15T12:00:00+00:00")


def test_parse_bronze_key_roundtrip() -> None:
    key = build_bronze_key("credits", "2026-09-15", "run_1", "27205")
    parsed = parse_bronze_key(key)
    assert parsed["resource"] == "credits"
    assert parsed["ingest_date"] == "2026-09-15"
    assert parsed["run_id"] == "run_1"
    assert parsed["filename"] == "27205.json"


def test_is_manifest_key() -> None:
    assert is_manifest_key("tmdb/movie/ingest_date=2026-09-15/run_id=r/550.manifest.json")
    assert not is_manifest_key("tmdb/movie/ingest_date=2026-09-15/run_id=r/550.json")
