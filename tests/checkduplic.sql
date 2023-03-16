select company_name, contact_name, count(1)
  from {{ref('customers')}}
group by 1,2  
having count(1) > 1