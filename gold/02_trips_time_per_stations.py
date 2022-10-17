# Databricks notebook source
from silver import *
from pyspark.sql.functions import sum, avg, col


GOLD_TABLE = "divvy.gold_trips_time_per_stations"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_trips")

# Analyze how much time is spent per ride Based on which station is the starting and / or ending station
df = (
    df.groupby(df.start_station_id, df.end_station_id)
    .agg(
        avg(df.time_spent).alias("avg_time_spent"),
        sum(df.time_spent).alias("sum_time_spent"),
    )
    .select(
        col("start_station_id"),
        col("end_station_id"),
        col("avg_time_spent"),
        col("sum_time_spent"),
    )
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
