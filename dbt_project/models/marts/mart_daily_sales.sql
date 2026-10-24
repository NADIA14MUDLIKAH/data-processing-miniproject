-- Agregat harian. Aturan bisnis: penjualan diakui hanya untuk status paid dan shipped.
select
    f.order_date,
    c.product_category,
    p.payment_method,
    count(*)                       as order_count,
    sum(f.quantity)                as units_sold,
    round(sum(f.gross_amount), 2)  as gross_sales,
    round(sum(f.net_amount), 2)    as net_sales
from {{ ref('fct_orders') }} f
join {{ ref('dim_product_category') }} c on f.category_id = c.category_id
join {{ ref('dim_payment_method') }} p on f.payment_method_id = p.payment_method_id
where f.is_recognized_sales
group by 1, 2, 3
