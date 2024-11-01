select *,
    quantity * price as revenue
from {{ ref('union_all_silver') }}