select
    company_id,
    max(name) as name,
    max(origin_country) as origin_country
from (
    select
        (c.value ->> 'id')::bigint as company_id,
        nullif(btrim(c.value ->> 'name'), '') as name,
        nullif(btrim(c.value ->> 'origin_country'), '') as origin_country
    from {{ source('bronze', 'raw_payloads') }} p
    cross join lateral jsonb_array_elements(
        coalesce(p.payload -> 'production_companies', '[]'::jsonb)
    ) as c
    where p.resource = 'movie'
      and c.value ->> 'id' is not null
) s
group by company_id
