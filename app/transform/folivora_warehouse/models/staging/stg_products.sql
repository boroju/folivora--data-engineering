select
    product_id,
    name,
    published,
    free_shipping,
    requires_shipping,
    created_at,
    updated_at
from
    {{ ref('raw_products') }}