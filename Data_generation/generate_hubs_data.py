'''
=============================================================
Generate data for hubs table
=============================================================
Script Purpose:
    - Generate artificial data for delivery hubs
    - Create a Spark DataFrame with the following columns: 
        hub_id,
        hub_name,
        city,
        hub_type
=============================================================
 '''

from datetime import date, datetime, timedelta
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
TODAY = date.today()
hubs = [
    (1, "Warsaw Hub", "Warsaw", "CENTRAL"),
    (2, "Krakow Hub", "Krakow", "REGIONAL"),
    (3, "Wroclaw Hub", "Wroclaw", "REGIONAL"),
    (4, "Poznan Hub", "Poznan", "REGIONAL"),
    (5, "Gdansk Hub", "Gdansk", "LOCAL")
]

columns = [
    "hub_id",
    "hub_name",
    "city",
    "hub_type"
]

df = spark.createDataFrame(hubs, columns)

df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"/Volumes/deliveries/default/generated_hubs_data/hubs_{TODAY.strftime('%Y%m%d')}")

print("dim_delivery_hub created")