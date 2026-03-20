with order_items_products as (
    select * from {{ ref('int_order_items_products') }}
),

order_items_geolocated as (
    select * from {{ ref('int_order_items_geolocated') }}
),

orders_enriched as (
    -- Kita butuh ini untuk mengambil status dan tanggal pembelian
    select 
        order_id, 
        customer_id, 
        order_status, 
        order_purchase_timestamp 
    from {{ ref('int_order_enriched') }}
)

select
    -- Primary Key (Kombinasi order_id dan order_item_id)
    {{ dbt_utils.generate_surrogate_key(['i.order_id', 'i.order_item_id']) }} as order_item_key,
    
    -- Foreign Keys
    i.order_id,
    o.customer_id,
    i.product_id,
    i.seller_id,
    
    -- Product & Category Info
    i.product_category_name,
    i.product_weight_g,
    i.product_volume_cm3,
    
    -- Financial Metrics
    i.price,
    i.freight_value,
    (i.price + i.freight_value) as total_item_value,
    
    -- Geospatial Metrics
    g.distance_km,
    g.customer_location,
    g.seller_location,
    
    -- Order Context
    o.order_status,
    o.order_purchase_timestamp,
    i.shipping_limit_date

from order_items_products i
left join order_items_geolocated g 
    on i.order_id = g.order_id 
    and i.seller_id = g.seller_id 
    and i.product_id = g.product_id
left join orders_enriched o 
    on i.order_id = o.order_id