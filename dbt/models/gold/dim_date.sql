with bounds as (
    select
        coalesce(min(release_date), date '1970-01-01') as dmin,
        coalesce(max(release_date), date '1970-01-01') as dmax
    from {{ ref('movie') }}
    where release_date is not null
)
select
    d::date as date_day,
    extract(year from d)::int as year,
    extract(month from d)::int as month,
    extract(day from d)::int as day,
    extract(quarter from d)::int as quarter,
    to_char(d, 'YYYY-MM') as year_month,
    to_char(d, 'Day') as day_name
from bounds
cross join generate_series(dmin, dmax, interval '1 day') as g(d)
