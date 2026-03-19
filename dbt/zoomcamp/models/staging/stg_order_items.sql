select
    cast(order_id as string) as order_id,
    cast(order_item_id as integer) as order_item_id,
    cast(product_id as string) as product_id,
    cast(seller_id as string) as seller_id,

    -- order item details
    cast(shipping_limit_date as timestamp) as shipping_limit_date,
    cast(price as float) as price,
    cast(freight_value as float) as freight_value
from {{ source('ecommerce_dataset', 'ext_order_items') }}
where order_id is not null and order_item_id is not null and product_id is not null and seller_id is not null