with orders as (
    select * from {{ ref('stg_orders') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
)

select
    o.*,
    c.customer_city,
    c.customer_state,
    -- Metrics
    date_diff(o.order_delivered_customer_date, o.order_purchase_timestamp, day) as delivery_time_in_days,
    date_diff(o.order_delivered_customer_date, o.order_estimated_delivery_date, day) as delay_from_estimate,

    -- Logistics Flags
    case when o.order_delivered_customer_date > o.order_estimated_delivery_date then true else false end as is_late,
    
    -- Time Dimensions
    {{ get_time_segment('o.order_purchase_timestamp') }} as purchase_time_segment,
    extract(month from o.order_purchase_timestamp) as purchase_month,
    extract(dayofweek from o.order_purchase_timestamp) as purchase_day_of_week,
    
    -- Customer Retention (Advanced)
    count(*) over (partition by o.customer_id) as total_customer_orders
from orders o
left join customers c on o.customer_id = c.customer_id
where o.order_status = 'delivered'