


SELECT 
    YEAR(timestamp) AS order_year,
    MONTH(timestamp) AS order_month,
    COUNT(id) AS total_orders,
    SUM(quantity) AS total_items_sold,
    SUM(revenue) AS total_revenue,
    AVG(price) AS avg_price,
    MAX(revenue) AS max_revenue,
    MIN(revenue) AS min_revenue,
    AVG(discount_applied) AS avg_discount_applied
FROM "ecom"."main"."revenue"
GROUP BY YEAR(timestamp),MONTH(timestamp)