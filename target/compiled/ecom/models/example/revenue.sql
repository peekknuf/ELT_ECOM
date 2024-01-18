-- Use the `ref` function to select from other models

select *, quantity * price as revenue
from "ecom"."main"."OBT_ECOM"