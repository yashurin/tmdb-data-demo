select distinct on (c.value ->> 'credit_id')
    (p.payload ->> 'id')::bigint as movie_id,
    (c.value ->> 'id')::bigint as person_id,
    c.value ->> 'credit_id' as credit_id,
    c.value ->> 'character' as character,
    nullif(c.value ->> 'order', '')::int as cast_order
from {{ source('bronze', 'raw_payloads') }} p
cross join lateral jsonb_array_elements(coalesce(p.payload -> 'cast', '[]'::jsonb)) as c
where p.resource = 'credits'
  and p.payload ->> 'id' is not null
  and c.value ->> 'id' is not null
  and nullif(c.value ->> 'credit_id', '') is not null
order by c.value ->> 'credit_id', p.extracted_at desc
