-- Satu baris per order_id (grain = pesanan).
select
    s.order_id,
    s.customer_id,
    c.category_id,
    p.payment_method_id,
    s.order_ts,
    s.order_date,
    s.quantity,
    s.unit_price,
    s.discount_pct,
    s.gross_amount,
    s.net_amount,
    s.status,
    s.status in ('paid', 'shipped') as is_recognized_sales
from {{ ref('stg_orders') }} s
join {{ ref('dim_product_category') }} c on s.product_category = c.product_category
join {{ ref('dim_payment_method') }} p on s.payment_method = p.payment_method
