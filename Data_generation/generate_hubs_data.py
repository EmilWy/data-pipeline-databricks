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

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
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

df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/Volumes/deliveries/default/generated_dim_data/gen_delivery_hub")

print("dim_delivery_hub created")