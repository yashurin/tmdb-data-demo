select
    iso_639_1,
    max(english_name) as english_name,
    max(name) as name
from (
    select
        nullif(btrim(l.value ->> 'iso_639_1'), '') as iso_639_1,
        nullif(btrim(l.value ->> 'english_name'), '') as english_name,
        nullif(btrim(l.value ->> 'name'), '') as name
    from {{ source('bronze', 'raw_payloads') }} p
    cross join lateral jsonb_array_elements(
        coalesce(p.payload -> 'spoken_languages', '[]'::jsonb)
    ) as l
    where p.resource = 'movie'
) s
where iso_639_1 is not null
group by iso_639_1
