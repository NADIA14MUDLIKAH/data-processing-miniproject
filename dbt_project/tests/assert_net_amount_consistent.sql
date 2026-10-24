-- Tes singular: gagal bila ada baris yang net_amount-nya tidak sesuai rumus.
select order_id
from {{ ref('fct_orders') }}
where abs(net_amount - quantity * unit_price * (1 - discount_pct)) > 0.005
