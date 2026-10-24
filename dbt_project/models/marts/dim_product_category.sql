select
    row_number() over (order by product_category) as category_id,
    product_category
from (select distinct product_category from {{ ref('stg_orders') }})
