# Databricks notebook source
from silver import *

SILVER_TABLE = "divvy.dim_stations"

spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE};")

df = spark.table("divvy.bronze_stations")

df = df.withColumn("latitute", df.latitute.cast("float"))
df = df.withColumn("longitude", df.longitude.cast("float"))

df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
