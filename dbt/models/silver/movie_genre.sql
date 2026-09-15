select distinct
    (p.payload ->> 'id')::bigint as movie_id,
    (g.value ->> 'id')::int as genre_id
from {{ source('bronze', 'raw_payloads') }} p
cross join lateral jsonb_array_elements(coalesce(p.payload -> 'genres', '[]'::jsonb)) as g
where p.resource = 'movie'
  and p.payload ->> 'id' is not null
  and g.value ->> 'id' is not null
