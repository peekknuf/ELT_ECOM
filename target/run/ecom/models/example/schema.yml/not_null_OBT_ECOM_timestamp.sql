select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select timestamp
from "ecom"."main"."OBT_ECOM"
where timestamp is null



      
    ) dbt_internal_test