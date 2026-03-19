select
    -- identifiers
    cast(product_id as string) as product_id,

    -- product details
    cast(product_category_name as string) as product_category_name,
    cast(product_name_length as integer) as product_name_length,
    cast(product_description_length as integer) as product_description_length,
    cast(product_photos_qty as integer) as product_photos_qty,
    cast(product_weight_g as float) as product_weight_g,
    cast(product_length_cm as float) as product_length_cm,
    cast(product_height_cm as float) as product_height_cm,
    cast(product_width_cm as float) as product_width_cm
from {{ source('ecommerce_dataset', 'ext_products') }}
where product_id is not null