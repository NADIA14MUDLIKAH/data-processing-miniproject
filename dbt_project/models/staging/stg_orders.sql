-- Staging: nama kolom dan tipe data dibuat konsisten. Tidak ada logika bisnis di sini.
select
    cast(order_id as varchar)            as order_id,
    cast(customer_id as varchar)         as customer_id,
    cast(order_ts as timestamp)          as order_ts,
    cast(order_ts as date)               as order_date,
    cast(product_category as varchar)    as product_category,
    cast(payment_method as varchar)      as payment_method,
    cast(quantity as integer)            as quantity,
    cast(unit_price as double)           as unit_price,
    cast(discount_pct as double)         as discount_pct,
    cast(gross_amount as double)         as gross_amount,
    cast(net_amount as double)           as net_amount,
    cast(status as varchar)              as status,
    cast(source as varchar)              as source
from {{ source('raw', 'raw_orders') }}
