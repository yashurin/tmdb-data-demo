select
    movie_id,
    company_id
from {{ ref('movie_company') }}
