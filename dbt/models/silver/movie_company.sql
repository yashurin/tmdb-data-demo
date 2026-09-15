select distinct
    (p.payload ->> 'id')::bigint as movie_id,
    (c.value ->> 'id')::bigint as company_id
from {{ source('bronze', 'raw_payloads') }} p
cross join lateral jsonb_array_elements(
    coalesce(p.payload -> 'production_companies', '[]'::jsonb)
) as c
where p.resource = 'movie'
  and p.payload ->> 'id' is not null
  and c.value ->> 'id' is not null
