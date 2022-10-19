# Databricks notebook source
from gold import *


GOLD_TABLE = "divvy.gold_trips_time_per_membertype"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_trips")
rider_df = spark.table("divvy.dim_riders")

# Analyze how much time is spent per ride Based on whether the rider is a member or a casual rider
df = df.join(rider_df, df.rider_id == rider_df.rider_id, "inner")
df = (
    df.groupby(df.is_member)
    .agg(
        F.avg(df.time_spent).alias("avg_time_spent"),
        F.sum(df.time_spent).alias("sum_time_spent"),
    )
    .select(
        F.col("is_member"),
        F.col("avg_time_spent"),
        F.col("sum_time_spent"),
    )
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
