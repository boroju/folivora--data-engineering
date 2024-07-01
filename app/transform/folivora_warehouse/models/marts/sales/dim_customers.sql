select
    customer_id
    ,name as customer_name
    ,active
    ,last_order_id
    ,first_interaction
    ,default_address_city as address_city
    ,default_address_locality as address_locality
    ,default_address_province as address_province
    ,default_address_zipcode as address_zipcode
    ,CAST(accepts_marketing AS BOOLEAN) as accepts_marketing
    ,strftime(created_at, '%Y%m%d') as date_created_iso
    ,strftime(updated_at, '%Y%m%d') as date_updated_iso
from
    {{ ref('stg_customers') }}