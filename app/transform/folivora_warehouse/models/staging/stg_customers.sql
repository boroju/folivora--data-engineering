select
    customer_id,
    name,
    active,
    last_order_id,
    first_interaction,
    created_at,
    updated_at,
    default_address_city,
    default_address_locality,
    default_address_province,
    default_address_zipcode,
    accepts_marketing
from
    {{ ref('raw_customers') }}