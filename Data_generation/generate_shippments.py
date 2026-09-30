'''
=============================================================
Generate data for shipments table
=============================================================
Script Purpose:
    - Generate artificial data for shippment table
    - Create a Spark DataFrame with the following columns:  
        shipment_id,
        customer_id,
        origin_hub_id,
        destination_hub_id,
        TODAY,
        package_weight,
        distance,
        shipping_method,
        status,
        delivery_cost
=============================================================
 '''

from pyspark.sql import SparkSession
from datetime import date, datetime, timedelta
import random

spark = SparkSession.builder.getOrCreate()
OUTPUT_PATH = "/Volumes/deliveries/default/generated_fact_data"

TODAY = date.today() + timedelta(days=1)
random.seed()
shipments = []

for shipment_id in range(1, 500):
    customer_id = random.randint(1, 5)
    origin_hub_id = random.randint(1, 5)
    destination_hub_id = random.randint(1, 5)

    while destination_hub_id == origin_hub_id:
        destination_hub_id = random.randint(1, 5)

    package_weight = round(
        random.uniform(0.5, 20.0),
        2
    )
    distance = random.randint(
        10,
        700
    )
    shipping_method = random.choice([
        "STANDARD",
        "EXPRESS"
    ])
    status = random.choice([
        "CREATED",
        "IN_TRANSIT",
        "DELIVERED"
    ])
    delivery_cost = round(
        10 + package_weight * 2 + distance * 0.02,
        2
    )
    shipments.append((
        shipment_id,
        customer_id,
        origin_hub_id,
        destination_hub_id,
        TODAY,
        package_weight,
        distance,
        shipping_method,
        status,
        delivery_cost
    ))

columns = [
    "shipment_id",
    "customer_id",
    "origin_hub_id",
    "destination_hub_id",
    "shipment_date",
    "package_weight_kg",
    "delivery_distance_km",
    "shipping_method",
    "status",
    "delivery_cost"
]

df = spark.createDataFrame(
    shipments,
    columns
)

file_name = f"shipments_{TODAY.strftime('%Y%m%d')}"

df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{OUTPUT_PATH}/{file_name}")

print(f"Created: {file_name}")