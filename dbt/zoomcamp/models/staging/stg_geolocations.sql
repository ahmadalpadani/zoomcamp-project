select
    cast(geolocation_zip_code_prefix as integer) as geolocation_zip_code_prefix,
    cast(geolocation_lat as float64) as geolocation_lat,
    cast(geolocation_lng as float64) as geolocation_lng,
    cast(geolocation_city as string) as geolocation_city,
    cast(geolocation_state as string) as geolocation_state
from {{ source('ecommerce_dataset', 'ext_geolocation') }}
where geolocation_zip_code_prefix is not null   