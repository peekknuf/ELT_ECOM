
  
  create view "ecom"."main"."aggregates_ordered__dbt_tmp" as (
    


select product_name, round(total_revenue,1) as total_revenue
from aggregates    
order by 2 desc
  );
