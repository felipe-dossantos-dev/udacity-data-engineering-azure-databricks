# Databricks notebook source
from gold import *


GOLD_TABLE = "divvy.gold_trips_time_per_age"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_trips")

# Analyze how much time is spent per ride Based on age of the rider at time of the ride
df = (
    df.groupby(df.rider_age_at_time)
    .agg(
        F.avg(df.time_spent).alias("avg_time_spent"),
        F.sum(df.time_spent).alias("sum_time_spent"),
    )
    .orderBy(df.rider_age_at_time)
    .select(
        F.col("rider_age_at_time"),
        F.col("avg_time_spent"),
        F.col("sum_time_spent"),
    )
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
