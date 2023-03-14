--customer model
with
markup as (
select *,
--first_value(customer_id) over(partition by company_name, contact_name order by company_name rows between unbounded preceding and unbounded following) as result,
row_number() over(partition by company_name, contact_name order by company_name) as pos
from {{source('sources','customers')}}
)

select * 
  from markup
 where pos = 1