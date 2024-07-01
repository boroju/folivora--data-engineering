select
    c.customer_id as customer_id
    ,c.customer_name as customer_name
    ,count(distinct o.order_id) as total_orders
    ,round(sum(o.discount), 2) as total_discount_made
    ,round(sum(o.total), 2) as total_spent_ars
    ,round(sum(o.total_usd), 2) as total_spent_usd
from
    {{ dbt_unit_testing.ref('fact_orders') }} o
    join {{ dbt_unit_testing.ref('dim_customers') }} c
    on o.customer_id = c.customer_id
where
    o.payment_status in ('paid','pending')
group by
    all
order by
    total_orders desc, total_spent_ars desc
