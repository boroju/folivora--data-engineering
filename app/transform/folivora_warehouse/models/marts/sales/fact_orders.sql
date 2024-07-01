select
    order_id
    ,customer_id
    ,coupon_id
    ,gateway_name
    ,storefront
    ,payment_status
    ,ROUND(cast(subtotal AS DOUBLE), 2) as subtotal
    ,ROUND(cast(discount AS DOUBLE), 2) as discount
    ,ROUND(cast(total AS DOUBLE), 2) as total
    ,ROUND(cast(total_usd AS DOUBLE), 2) as total_usd
    ,strftime(created_at, '%Y%m%d') as date_created_iso
    ,strftime(updated_at, '%Y%m%d') as date_updated_iso
    ,strftime(cast(completed_at_date AS TIMESTAMP), '%Y%m%d') as date_completed_iso
    ,strftime(paid_at, '%Y%m%d') as date_paid_iso
from
    {{ ref('stg_orders') }}
