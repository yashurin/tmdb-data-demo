with genre_lists as (
    select
        mg.movie_id,
        string_agg(g.name, ', ' order by g.name) as genre_list
    from {{ ref('movie_genre') }} mg
    inner join {{ ref('genre') }} g on g.genre_id = mg.genre_id
    group by mg.movie_id
),
top_cast as (
    select
        cc.movie_id,
        string_agg(p.name, ', ' order by cc.cast_order) as top_3_cast
    from {{ ref('credit_cast') }} cc
    inner join {{ ref('person') }} p on p.person_id = cc.person_id
    where cc.cast_order is not null
      and cc.cast_order < 3
    group by cc.movie_id
),
primary_company as (
    select distinct on (mc.movie_id)
        mc.movie_id,
        c.name as primary_company
    from {{ ref('movie_company') }} mc
    inner join {{ ref('company') }} c on c.company_id = mc.company_id
    order by mc.movie_id, mc.company_id
)
select
    m.movie_id,
    m.title,
    m.original_title,
    m.release_date,
    m.original_language,
    m.status,
    m.runtime,
    m.budget,
    m.revenue,
    m.revenue - m.budget as profit,
    case
        when m.budget > 0 then (m.revenue - m.budget)::numeric / m.budget
        else null
    end as roi,
    m.vote_average,
    m.vote_count,
    m.popularity,
    gl.genre_list,
    tc.top_3_cast,
    pc.primary_company
from {{ ref('movie') }} m
left join genre_lists gl on gl.movie_id = m.movie_id
left join top_cast tc on tc.movie_id = m.movie_id
left join primary_company pc on pc.movie_id = m.movie_id
