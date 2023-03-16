select se.shipper_email,
       sh.company_name
  from {{source('sources', 'shippers')}} sh
  left join {{ref('shippers_emails')}} se on (sh.shipper_id = se.shipper_id)