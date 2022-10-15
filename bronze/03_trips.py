# Databricks notebook source
from bronze import *

BRONZE_TABLE = "divvy.bronze_trips"

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE};")
df = spark.read.format("csv").option("sep", ",").load("/FileStore/raw_data/trips.csv")

df = df.withColumn("trip_id", df._c0)
df = df.withColumn("rideable_type", df._c1)
df = df.withColumn("start_at", df._c2.cast("timestamp"))
df = df.withColumn("ended_at", df._c3.cast("timestamp"))

df = df.withColumn("start_station_id", df._c4)
df = df.withColumn("end_station_id", df._c5)
df = df.withColumn("rider_id", df._c6.cast("int"))

columns_to_drop = ["_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6"]
df = df.drop(*columns_to_drop)

df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)
