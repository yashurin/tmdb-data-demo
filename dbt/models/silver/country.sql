select
    iso_3166_1,
    max(name) as name
from (
    select
        nullif(btrim(c.value ->> 'iso_3166_1'), '') as iso_3166_1,
        nullif(btrim(c.value ->> 'name'), '') as name
    from {{ source('bronze', 'raw_payloads') }} p
    cross join lateral jsonb_array_elements(
        coalesce(p.payload -> 'production_countries', '[]'::jsonb)
    ) as c
    where p.resource = 'movie'
) s
where iso_3166_1 is not null
group by iso_3166_1
