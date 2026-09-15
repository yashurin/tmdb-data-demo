select
    company_id,
    name as company_name,
    origin_country
from {{ ref('company') }}
