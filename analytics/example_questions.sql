-- Example Metabase / psql questions against warehouse.gold
-- Connect: host=postgres (from Metabase container) or localhost (from host),
--          database=warehouse, user/password from .env, schema=gold

-- 1. Top movies by vote_average with a minimum vote_count
select
    title,
    release_date,
    vote_average,
    vote_count,
    genre_list
from gold.movie_analytics
where vote_count >= 500
order by vote_average desc, vote_count desc
limit 20;

-- 2. Revenue vs budget by release year
select
    extract(year from release_date)::int as release_year,
    count(*) as movies,
    sum(budget) as total_budget,
    sum(revenue) as total_revenue,
    sum(profit) as total_profit
from gold.movie_analytics
where release_date is not null
group by 1
order by 1;

-- 3. Genre popularity (movie counts and average rating)
select
    g.genre_name,
    count(distinct f.movie_id) as movie_count,
    round(avg(f.vote_average), 2) as avg_vote,
    sum(f.revenue) as total_revenue
from gold.bridge_movie_genre b
join gold.dim_genre g on g.genre_id = b.genre_id
join gold.fact_movie f on f.movie_id = b.movie_id
group by g.genre_name
order by movie_count desc;

-- 4. Most credited actors in the ingested set
select
    p.name,
    count(*) as cast_credits,
    count(distinct c.movie_id) as movies
from gold.fact_movie_cast c
join gold.dim_person p on p.person_id = c.person_id
group by p.name
order by cast_credits desc, movies desc
limit 25;

-- 5. Crew department distribution
select
    department,
    count(*) as credits,
    count(distinct movie_id) as movies,
    count(distinct person_id) as people
from gold.fact_movie_crew
group by department
order by credits desc;

-- 6. Companies with highest total revenue
select
    d.company_name,
    count(distinct b.movie_id) as movies,
    sum(f.revenue) as total_revenue,
    sum(f.budget) as total_budget
from gold.bridge_movie_company b
join gold.dim_company d on d.company_id = b.company_id
join gold.fact_movie f on f.movie_id = b.movie_id
group by d.company_name
order by total_revenue desc nulls last
limit 20;

-- 7. Movies with profit < 0
select
    title,
    release_date,
    budget,
    revenue,
    profit,
    roi,
    primary_company
from gold.movie_analytics
where profit < 0
  and budget > 0
order by profit asc
limit 20;

-- 8. Original-language mix
select
    original_language,
    count(*) as movies,
    round(avg(vote_average), 2) as avg_vote,
    sum(revenue) as total_revenue
from gold.movie_analytics
group by original_language
order by movies desc;
