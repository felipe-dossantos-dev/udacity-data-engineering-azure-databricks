# Databricks notebook source
from silver import *
from pyspark.sql.functions import sum, avg, col


GOLD_TABLE = "divvy.gold_trips_time_per_membertype"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_trips")
rider_df = spark.table("divvy.dim_riders")

# Analyze how much time is spent per ride Based on whether the rider is a member or a casual rider
df = df.join(rider_df, df.rider_id == rider_df.rider_id, "inner")
df = (
    df.groupby(df.is_member)
    .agg(
        avg(df.time_spent).alias("avg_time_spent"),
        sum(df.time_spent).alias("sum_time_spent"),
    )
    .select(
        col("is_member"),
        col("avg_time_spent"),
        col("sum_time_spent"),
    )
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
