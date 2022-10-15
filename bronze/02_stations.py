# Databricks notebook source
from bronze import *

BRONZE_TABLE = "divvy.bronze_stations"

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE};")
df = (
    spark.read.format("csv").option("sep", ",").load("/FileStore/raw_data/stations.csv")
)

df = df.withColumn("station_id", df._c0)
df = df.withColumn("name", df._c1)
df = df.withColumn("latitute", df._c2.cast("float"))
df = df.withColumn("longitude", df._c3.cast("float"))

columns_to_drop = ["_c0", "_c1", "_c2", "_c3"]
df = df.drop(*columns_to_drop)

df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)
