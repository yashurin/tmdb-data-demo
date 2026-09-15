select distinct
    (p.payload ->> 'id')::bigint as movie_id,
    nullif(btrim(c.value ->> 'iso_3166_1'), '') as iso_3166_1
from {{ source('bronze', 'raw_payloads') }} p
cross join lateral jsonb_array_elements(
    coalesce(p.payload -> 'production_countries', '[]'::jsonb)
) as c
where p.resource = 'movie'
  and p.payload ->> 'id' is not null
  and nullif(btrim(c.value ->> 'iso_3166_1'), '') is not null
