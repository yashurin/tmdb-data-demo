from bronze_catalog import tmdb_id_from_payload


def test_tmdb_id_from_movie_payload() -> None:
    assert tmdb_id_from_payload("movie", {"id": 27205, "title": "Inception"}, "27205.json") == 27205


def test_tmdb_id_from_page_payload() -> None:
    assert tmdb_id_from_payload("movie_popular", {"page": 3, "results": []}, "page_003.json") == 3


def test_tmdb_id_genre_list_is_zero() -> None:
    assert tmdb_id_from_payload("genre", {"genres": []}, "genre_movie_list.json") == 0
