select *
from {{ ref('input') }}
where id = 3