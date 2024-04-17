import os

from random_data_gen import generate_batch_data, write_csv


def create_new_csv(base_folder, base_file_name, fieldnames):
    existing_files = [
        f
        for f in os.listdir(base_folder)
        if os.path.isfile(os.path.join(base_folder, f))
    ]
    index = len(existing_files) + 1

    file_name = os.path.join(base_folder, base_file_name.format(index))
    num_records = 10000

    batch_size = 5000
    num_batches = num_records // batch_size

    data = []
    for _ in range(num_batches):
        data.extend(generate_batch_data(batch_size))

    write_csv(file_name, fieldnames, data)


if __name__ == "__main__":
    base_folder = "dbt/data_pipeline/post"
    base_file_name = "ecommerce_data_{}.csv"
    field_names = [
        "id",
        "timestamp",
        "product_name",
        "price",
        "quantity",
        "category",
        "customer_name",
        "email",
        "address",
        "country",
        "payment_method",
        "phone_number",
        "discount_applied",
        "shipping_method",
        "order_status",
        "customer_age",
    ]

    create_new_csv(base_folder, base_file_name, field_names)
