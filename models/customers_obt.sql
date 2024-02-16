SELECT customer_name,
    array(
        customer_age,
        email,
        address,
        country,
        phone_number
    ) as customers_array
from { { ref('union_all') } }