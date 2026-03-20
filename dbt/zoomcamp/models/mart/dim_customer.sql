with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_orders as (
    select 
        c.customer_unique_id,
        count(distinct o.order_id) as total_orders,
        sum(o.total_order_value) as total_lifetime_value,
        min(o.order_purchase_timestamp) as first_purchase_at,
        max(o.order_purchase_timestamp) as last_purchase_at
    from {{ ref('fct_orders') }} o
    join customers c on o.customer_id = c.customer_id
    group by 1
)

select
    c.customer_unique_id,
    -- Menggunakan MAX untuk mengambil lokasi terbaru jika ada duplikat unique_id
    max(c.customer_city) as customer_city,
    max(c.customer_state) as customer_state,
    
    coalesce(co.total_orders, 0) as total_orders,
    coalesce(co.total_lifetime_value, 0) as total_lifetime_value,
    co.first_purchase_at,
    co.last_purchase_at,
    
    -- Macro untuk segmentasi
    {{ get_customer_loyalty('co.total_orders') }} as loyalty_segment

from customers c
left join customer_orders co on c.customer_unique_id = co.customer_unique_id
group by 
    c.customer_unique_id, 
    co.total_orders, 
    co.total_lifetime_value, 
    co.first_purchase_at, 
    co.last_purchase_at,
    8 -- Kolom ke-8 adalah hasil Macro (loyalty_segment)