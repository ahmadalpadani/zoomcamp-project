with items as (
    select * from {{ ref('stg_order_items') }}
),

reviews as (
    select * from {{ ref('stg_reviews') }}
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

-- Agregasi review di level order agar tidak bikin duplikat saat join ke items
order_reviews as (
    select 
        order_id,
        avg(review_score) as avg_order_score
    from reviews
    group by 1
)

select 
    i.seller_id,
    -- Pastikan ini string agar konsisten dengan tabel geo
    safe_cast(s.seller_zip_code_prefix as string) as seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    
    count(distinct i.order_id) as total_orders,
    sum(i.price) as total_revenue,
    avg(r.avg_order_score) as average_seller_rating,
    count(distinct r.order_id) as total_reviews,
    
    -- Pastikan menggunakan DATE() untuk menghindari error tipe data
    avg(date_diff(date(i.shipping_limit_date), date(o.order_purchase_timestamp), day)) as avg_shipping_limit_buffer_days
    
from items i
left join order_reviews r on i.order_id = r.order_id
left join sellers s on i.seller_id = s.seller_id
left join orders o on i.order_id = o.order_id
group by 1, 2, 3, 4