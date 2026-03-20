select
    cast(product_category_name as string) as product_category_name,
    cast(product_category_name_english as string) as product_category_name_english
from {{ source('ecommerce_dataset', 'ext_product_category_translation') }}
where product_category_name is not null 