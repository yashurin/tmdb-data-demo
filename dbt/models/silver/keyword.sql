select
    keyword_id,
    max(name) as name
from (
    select
        (k.value ->> 'id')::bigint as keyword_id,
        nullif(btrim(k.value ->> 'name'), '') as name
    from {{ source('bronze', 'raw_payloads') }} p
    cross join lateral jsonb_array_elements(coalesce(p.payload -> 'keywords', '[]'::jsonb)) as k
    where p.resource = 'keywords'
      and k.value ->> 'id' is not null
) s
group by keyword_id
