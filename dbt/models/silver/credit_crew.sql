select distinct on (c.value ->> 'credit_id')
    (p.payload ->> 'id')::bigint as movie_id,
    (c.value ->> 'id')::bigint as person_id,
    c.value ->> 'credit_id' as credit_id,
    nullif(btrim(c.value ->> 'department'), '') as department,
    nullif(btrim(c.value ->> 'job'), '') as job
from {{ source('bronze', 'raw_payloads') }} p
cross join lateral jsonb_array_elements(coalesce(p.payload -> 'crew', '[]'::jsonb)) as c
where p.resource = 'credits'
  and p.payload ->> 'id' is not null
  and c.value ->> 'id' is not null
  and nullif(c.value ->> 'credit_id', '') is not null
order by c.value ->> 'credit_id', p.extracted_at desc
