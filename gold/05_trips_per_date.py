# Databricks notebook source
from silver import *
import pyspark.sql.functions as F


GOLD_TABLE = "divvy.gold_trips_per_date"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_trips")
# Analyze how much time is spent per ride Based on date and time factors such as day of week and time of day
date_df = spark.table("divvy.dim_date")

df = df.join(date_df, df.start_at_date == date_df.date, "inner")
df = df.withColumn("time_15_min", F.floor(F.minute(df.start_at) / 15) + 1)

df = (
    df.groupby(df.day_of_week, df.time_15_min)
    .agg(
        F.avg(df.time_spent).alias("avg_time_spent"),
        F.sum(df.time_spent).alias("sum_time_spent"),
    )
    .orderBy(df.day_of_week, df.time_15_min)
    .select(
        F.col("day_of_week"),
        F.col("time_15_min"),
        F.col("avg_time_spent"),
        F.col("sum_time_spent"),
    )
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
