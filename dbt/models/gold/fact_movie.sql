select
    m.movie_id,
    m.release_date as date_day,
    m.budget,
    m.revenue,
    m.runtime,
    m.vote_count,
    m.popularity,
    m.vote_average,
    m.revenue - m.budget as profit,
    case
        when m.budget > 0 then (m.revenue - m.budget)::numeric / m.budget
        else null
    end as roi
from {{ ref('movie') }} m
