select
    keyword_id,
    name as keyword_name
from {{ ref('keyword') }}
