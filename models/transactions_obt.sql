WITH transaction_data AS (
  SELECT
    ID,
    timestamp,
    product_name,
    CAST(price AS VARCHAR) AS price,
    CAST(quantity AS VARCHAR) AS quantity,
    category,
    payment_method,
    discount_applied,
    shipping_method,
    order_status
  FROM {{ ref('union_all') }}
)
SELECT
  ID,
  ARRAY[timestamp, product_name, price, quantity, category, payment_method, discount_applied, shipping_method, order_status] AS transactions_array
FROM transaction_data