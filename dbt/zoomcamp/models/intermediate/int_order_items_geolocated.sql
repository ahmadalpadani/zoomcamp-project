with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
), 

customer_geo as (
    select
        c.customer_id,
        c.customer_zip_code_prefix,
        -- Gunakan alias g agar jelas asal kolomnya
        st_geogpoint(avg(g.geolocation_lng), avg(g.geolocation_lat)) as customer_location
    from {{ ref('stg_customers') }} c
    left join {{ ref('stg_geolocations') }} g on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
    group by 1, 2
),

seller_geo as (
    select
        s.seller_id,
        s.seller_zip_code_prefix,
        st_geogpoint(avg(g.geolocation_lng), avg(g.geolocation_lat)) as seller_location
    from {{ ref('stg_sellers') }} s
    left join {{ ref('stg_geolocations') }} g on s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
    group by 1, 2
)

select 
    i.order_id,
    o.customer_id, -- Tambahkan ini agar bisa tracking per user
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    i.shipping_limit_date,
    o.order_purchase_timestamp,
    c.customer_location,
    s.seller_location,
    -- Menghitung jarak antara pelanggan dan penjual dalam kilometer
    st_distance(c.customer_location, s.seller_location) / 1000 as distance_km
from items i
join orders o on i.order_id = o.order_id -- Gunakan JOIN karena item pasti punya order
left join customer_geo c on o.customer_id = c.customer_id
left join seller_geo s on i.seller_id = s.seller_id
where o.order_status = 'delivered' 
  and o.order_delivered_customer_date is not null