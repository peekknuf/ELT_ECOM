WITH customer_data AS (
  SELECT
    customer_name,
    CAST(customer_age AS VARCHAR) AS customer_age,
    email,
    address,
    country,
    phone_number
  FROM {{ ref('union_all') }}
)
SELECT
  customer_name,
  ARRAY[customer_age, email, address, country, phone_number] AS customers_array
FROM customer_data