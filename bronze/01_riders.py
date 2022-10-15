# Databricks notebook source
from bronze import *

BRONZE_TABLE = "divvy.bronze_riders"

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE};")
df = spark.read.format("csv").option("sep", ",").load("/FileStore/raw_data/riders.csv")

df = df.withColumn("rider_id", df._c0)
df = df.withColumn("first", df._c1)
df = df.withColumn("last", df._c2)
df = df.withColumn("address", df._c3)
df = df.withColumn("birthday", df._c4)
df = df.withColumn("account_start_date", df._c5)
df = df.withColumn("account_end_date", df._c6)
df = df.withColumn("is_member", df._c7)


columns_to_drop = ["_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7"]
df = df.drop(*columns_to_drop)

df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)
