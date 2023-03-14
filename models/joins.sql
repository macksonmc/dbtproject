with
prod as (
    select ct.category_name,
        sp.company_name as suppliers,
        pd.product_name,
        pd.unit_price,
        pd.product_id
    from {{source('sources', 'products')}} pd
    left join {{source('sources', 'suppliers')}} sp on (pd.supplier_id = sp.supplier_id)
    left join {{source('sources', 'categories')}} ct on (ct.category_id = pd.category_id)
),
orderdetail as (
    select pd.*,
           od.order_id,
           od.quantity,
           od.discount
      from {{ref('orderdetails')}} od
      left join prod pd on (od.product_id = od.product_id)
),

orderss as (
    select ord.order_date,
           ord.order_id,
           cs.company_name customer,
           em.full_name employee,
           em.age,
           em.lengthofservice
      from {{source('sources', 'orders')}} ord
      left join {{ref('customers')}} cs on cs.customer_id = ord.customer_id
      left join {{ref('employees')}} em on em.employee_id = ord.employee_id
      left join {{source('sources', 'shippers')}} sh on ord.ship_via = sh.shipper_id
),

finaljoin as (
    select od.*,
           ord.order_date,
           ord.customer,
           ord.employee,
           ord.age,
           ord.lengthofservice
      from orderdetail od
      inner join orderss ord on (od.order_id = ord.order_id)
)

select *
  from finaljoin