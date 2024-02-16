{ { config(materialized = 'table') } }
SELECT ID,
    array(
        timestamp,
        product_name,
        price,
        quantity,
        revenue,
        category,
        payment_method,
        discount_applied,
        shipping_method,
        order_status
    ) as transactions_array
FROM { { ref('union_all') } }