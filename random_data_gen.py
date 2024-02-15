import csv
import os
from faker import Faker
import random
from multiprocessing import Pool

fake = Faker()


def generate_batch_data(batch_size):
    data = []
    for _ in range(batch_size):
        data.append(
            {
                "id": fake.uuid4(),
                "timestamp": fake.date_time_this_decade(),
                "product_name": fake.word(),
                "price": round(random.uniform(10, 1000), 2),
                "quantity": random.randint(1, 100),
                "category": fake.word(),
                "customer_name": fake.name(),
                "email": fake.email(),
                "address": fake.address(),
                "country": fake.country(),
                "payment_method": fake.random_element(
                    elements=("Credit Card", "Cash", "PayPal")
                ),
                "phone_number": fake.phone_number(),
                "discount_applied": round(random.uniform(0, 100), 2),
                "shipping_method": fake.random_element(
                    elements=("Standard Shipping", "Express Shipping")
                ),
                "order_status": fake.random_element(
                    elements=("Pending", "Shipped", "Delivered")
                ),
                "customer_age": random.randint(18, 80),
            }
        )
    return data


def write_csv(file_name, fieldnames, data):
    with open(file_name, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def create_csv_file(file_name, fieldnames, num_records):
    """Generate data in parallel and write to CSV"""
    batch_size = 10000
    num_batches = num_records // batch_size

    with Pool() as pool:
        batch_data = pool.map(generate_batch_data, [batch_size] * num_batches)
        flattened_data = [record for batch in batch_data for record in batch]
        write_csv(file_name, fieldnames, flattened_data)


if __name__ == "__main__":
    base_folder = "seeds"
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

    for i in range(1, 12):
        """the n of files is arbitrary"""
        file_name = os.path.join(base_folder, base_file_name.format(i))
        create_csv_file(file_name, field_names, num_records=1_000_000)
