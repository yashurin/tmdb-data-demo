select
    cc.credit_id,
    cc.movie_id,
    cc.person_id,
    cc.character,
    cc.cast_order
from {{ ref('credit_cast') }} cc
