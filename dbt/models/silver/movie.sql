{{
    config(
        materialized="incremental",
        unique_key="movie_id",
        incremental_strategy="merge",
    )
}}

with ranked as (
    select
        (p.payload ->> 'id')::bigint as movie_id,
        nullif(btrim(p.payload ->> 'title'), '') as title,
        nullif(btrim(p.payload ->> 'original_title'), '') as original_title,
        p.payload ->> 'overview' as overview,
        nullif(btrim(p.payload ->> 'release_date'), '')::date as release_date,
        nullif(p.payload ->> 'runtime', '')::int as runtime,
        nullif(btrim(p.payload ->> 'status'), '') as status,
        nullif(btrim(p.payload ->> 'original_language'), '') as original_language,
        nullif(p.payload ->> 'popularity', '')::numeric as popularity,
        nullif(p.payload ->> 'vote_average', '')::numeric as vote_average,
        nullif(p.payload ->> 'vote_count', '')::int as vote_count,
        coalesce(nullif(p.payload ->> 'budget', '')::bigint, 0) as budget,
        coalesce(nullif(p.payload ->> 'revenue', '')::bigint, 0) as revenue,
        coalesce((p.payload ->> 'adult')::boolean, false) as adult,
        p.payload ->> 'poster_path' as poster_path,
        p.payload ->> 'backdrop_path' as backdrop_path,
        p.extracted_at as loaded_at,
        row_number() over (
            partition by (p.payload ->> 'id')::bigint
            order by p.extracted_at desc
        ) as rn
    from {{ source('bronze', 'raw_payloads') }} p
    where p.resource = 'movie'
      and p.payload ->> 'id' is not null
      {% if is_incremental() %}
      and p.extracted_at > (select coalesce(max(loaded_at), '1970-01-01'::timestamptz) from {{ this }})
      {% endif %}
)
select
    movie_id,
    title,
    original_title,
    overview,
    release_date,
    runtime,
    status,
    original_language,
    popularity,
    vote_average,
    vote_count,
    budget,
    revenue,
    adult,
    poster_path,
    backdrop_path,
    loaded_at
from ranked
where rn = 1
