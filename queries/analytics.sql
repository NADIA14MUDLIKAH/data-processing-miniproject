-- name: Q1 Penjualan bersih per hari (status paid/shipped)
SELECT order_date, round(SUM(net_sales), 2) AS net_sales
FROM mart_daily_sales
GROUP BY order_date
ORDER BY order_date;

-- name: Q2 Penjualan bersih per kategori produk
SELECT product_category, round(SUM(net_sales), 2) AS net_sales
FROM mart_daily_sales
GROUP BY product_category
ORDER BY net_sales DESC;

-- name: Q3 Jumlah dan nilai pesanan menurut status (semua record valid)
SELECT status, COUNT(*) AS order_count, round(SUM(net_amount), 2) AS net_amount
FROM fct_orders
GROUP BY status
ORDER BY order_count DESC;

-- name: Q4 Frekuensi metode pembayaran (semua record valid)
SELECT p.payment_method, COUNT(*) AS order_count
FROM fct_orders f
JOIN dim_payment_method p ON f.payment_method_id = p.payment_method_id
GROUP BY p.payment_method
ORDER BY order_count DESC;

-- name: Q5a Jumlah record ditolak (data contract, batch)
SELECT COUNT(*) AS rejected_count
FROM read_parquet('data/rejected/orders_rejected.parquet');

-- name: Q5b Penyebab penolakan (dikelompokkan menurut field)
SELECT error_fields, COUNT(*) AS rejected_count
FROM read_parquet('data/rejected/orders_rejected.parquet')
GROUP BY error_fields
ORDER BY rejected_count DESC, error_fields;
