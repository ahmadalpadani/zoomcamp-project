with items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

products_category as (
    select * from {{ ref('stg_product_categories') }}
) -- Koma di sini sudah dihapus

select 
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    i.shipping_limit_date,
    -- Ambil dari alias 'pc' untuk bahasa Inggris
    coalesce(pc.product_category_name_english, p.product_category_name) as product_category_name,
    -- Pastikan nama kolom berat sudah benar
    p.product_weight_g, 
    p.product_length_cm * p.product_height_cm * p.product_width_cm as product_volume_cm3
from items i
join products p on i.product_id = p.product_id
left join products_category pc on p.product_category_name = pc.product_category_name