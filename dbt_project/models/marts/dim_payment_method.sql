select
    row_number() over (order by payment_method) as payment_method_id,
    payment_method
from (select distinct payment_method from {{ ref('stg_orders') }})
