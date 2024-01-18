/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/



select *
from econ0
union all
select *
from econ1
union all
select *
from econ2
union all
select *
from econ3
union all
select *
from econ4
union all
select *
from econ5
union all
select *
from econ6
union all
select *
from econ7
union all
select *
from econ8
union all
select *
from econ9
union all
select *
from econ10

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null