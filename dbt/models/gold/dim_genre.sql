select
    genre_id,
    name as genre_name
from {{ ref('genre') }}
