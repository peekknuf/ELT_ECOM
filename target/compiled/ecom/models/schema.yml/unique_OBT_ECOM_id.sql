
    
    

select
    id as unique_field,
    count(*) as n_records

from "ecom"."main"."OBT_ECOM"
where id is not null
group by id
having count(*) > 1


