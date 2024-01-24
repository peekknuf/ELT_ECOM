select *, quantity * price as revenue
from {{ ref('OBT_ECOM') }}

