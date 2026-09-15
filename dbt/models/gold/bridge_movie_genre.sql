select
    movie_id,
    genre_id
from {{ ref('movie_genre') }}
