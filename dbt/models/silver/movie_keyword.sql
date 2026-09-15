select distinct
    (p.payload ->> 'id')::bigint as movie_id,
    (k.value ->> 'id')::bigint as keyword_id
from {{ source('bronze', 'raw_payloads') }} p
cross join lateral jsonb_array_elements(coalesce(p.payload -> 'keywords', '[]'::jsonb)) as k
where p.resource = 'keywords'
  and p.payload ->> 'id' is not null
  and k.value ->> 'id' is not null
