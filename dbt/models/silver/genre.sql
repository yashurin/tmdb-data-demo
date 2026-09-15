{{ config(unique_key="genre_id") }}

with from_list as (
    select
        (g.value ->> 'id')::int as genre_id,
        nullif(btrim(g.value ->> 'name'), '') as name
    from {{ source('bronze', 'raw_payloads') }} p
    cross join lateral jsonb_array_elements(coalesce(p.payload -> 'genres', '[]'::jsonb)) as g
    where p.resource = 'genre'
),
from_movies as (
    select
        (g.value ->> 'id')::int as genre_id,
        nullif(btrim(g.value ->> 'name'), '') as name
    from {{ source('bronze', 'raw_payloads') }} p
    cross join lateral jsonb_array_elements(coalesce(p.payload -> 'genres', '[]'::jsonb)) as g
    where p.resource = 'movie'
),
unioned as (
    select * from from_list
    union
    select * from from_movies
)
select
    genre_id,
    max(name) as name
from unioned
where genre_id is not null
group by genre_id
