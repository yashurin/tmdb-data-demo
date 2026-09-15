select
    movie_id,
    title,
    original_title,
    overview,
    release_date,
    runtime,
    status,
    original_language,
    adult,
    poster_path,
    backdrop_path
from {{ ref('movie') }}
