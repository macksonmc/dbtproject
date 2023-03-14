with
calc_employess as (
select first_name || ' ' ||  last_name as full_name,
       date_part(year, current_date) - date_part(year, birth_date) as age,
       date_part(year, current_date) - date_part(year, hire_date) as lengthofservice,
       *
  from {{source('sources', 'employees')}}
)

select *
  from calc_employess