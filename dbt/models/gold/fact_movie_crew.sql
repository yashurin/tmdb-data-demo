select
    cr.credit_id,
    cr.movie_id,
    cr.person_id,
    cr.department,
    cr.job
from {{ ref('credit_crew') }} cr
