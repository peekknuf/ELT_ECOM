
  
  create view "ecom"."main"."revenue__dbt_tmp" as (
    -- Use the `ref` function to select from other models

select *, quantity * price as revenue
from "ecom"."main"."OBT_ECOM"
  );
