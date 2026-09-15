{{
    config(
        materialized="incremental",
        unique_key="person_id",
        incremental_strategy="merge",
    )
}}

with from_person as (
    select
        (p.payload ->> 'id')::bigint as person_id,
        nullif(btrim(p.payload ->> 'name'), '') as name,
        nullif(p.payload ->> 'gender', '')::int as gender,
        nullif(btrim(p.payload ->> 'known_for_department'), '') as known_for_department,
        nullif(p.payload ->> 'popularity', '')::numeric as popularity,
        nullif(btrim(p.payload ->> 'birthday'), '')::date as birthday,
        nullif(btrim(p.payload ->> 'deathday'), '')::date as deathday,
        nullif(btrim(p.payload ->> 'place_of_birth'), '') as place_of_birth,
        p.payload ->> 'biography' as biography,
        1 as source_rank,
        p.extracted_at
    from {{ source('bronze', 'raw_payloads') }} p
    where p.resource = 'person'
      and p.payload ->> 'id' is not null
),
from_credits as (
    select distinct on ((c.value ->> 'id')::bigint)
        (c.value ->> 'id')::bigint as person_id,
        nullif(btrim(c.value ->> 'name'), '') as name,
        nullif(c.value ->> 'gender', '')::int as gender,
        nullif(btrim(c.value ->> 'known_for_department'), '') as known_for_department,
        nullif(c.value ->> 'popularity', '')::numeric as popularity,
        null::date as birthday,
        null::date as deathday,
        null::text as place_of_birth,
        null::text as biography,
        2 as source_rank,
        p.extracted_at
    from {{ source('bronze', 'raw_payloads') }} p
    cross join lateral jsonb_array_elements(
        coalesce(p.payload -> 'cast', '[]'::jsonb) || coalesce(p.payload -> 'crew', '[]'::jsonb)
    ) as c
    where p.resource = 'credits'
      and c.value ->> 'id' is not null
    order by
        (c.value ->> 'id')::bigint,
        (c.value ->> 'popularity')::numeric desc nulls last
),
unioned as (
    select * from from_person
    union all
    select * from from_credits
),
ranked as (
    select
        *,
        row_number() over (
            partition by person_id
            order by source_rank, extracted_at desc
        ) as rn
    from unioned
)
select
    person_id,
    name,
    gender,
    known_for_department,
    popularity,
    birthday,
    deathday,
    place_of_birth,
    biography
from ranked
where rn = 1
