select
    person_id,
    name,
    gender,
    known_for_department,
    popularity,
    birthday,
    deathday,
    place_of_birth
from {{ ref('person') }}
