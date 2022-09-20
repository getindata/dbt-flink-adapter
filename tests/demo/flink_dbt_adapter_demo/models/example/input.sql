-- TODO: use kafka topic as source
select *
from {{ ref('test_messages') }}
