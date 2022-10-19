# Databricks notebook source
from gold import *

GOLD_TABLE = "divvy.gold_payments_per_rides_minutes"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_payments")
date_df = spark.table("divvy.dim_date").select(
    F.col("date"),
    F.col("month"),
    F.col("year"),
)
df = df.join(date_df, df.date == date_df.date, "inner")

# on how many minutes the rider spends on a bike per month
trips_df = spark.table("divvy.fact_trips")
trips_df = trips_df.join(date_df, trips_df.start_at_date == date_df.date, "inner")
trips_df = (
    trips_df.groupby(trips_df.rider_id, trips_df.month, trips_df.year)
    .agg(
        F.sum(trips_df.time_spent).alias("sum_time_spent"),
    )
    .orderBy("rider_id", "year", "month")
    .select(
        F.col("rider_id").alias("trips_rider_id"),
        F.col("month").alias("trips_month"),
        F.col("year").alias("trips_year"),
        F.col("sum_time_spent"),
    )
)

# Analyze how much money is spent per member Based on how many minutes the rider spends on a bike per month
df = (
    df.groupby(df.rider_id, df.month, df.year)
    .agg(
        F.sum(df.amount).alias("sum_amount"),
    )
    .orderBy("rider_id", "year", "month")
    .select(
        F.col("rider_id"),
        F.col("month"),
        F.col("year"),
        F.col("sum_amount"),
    )
)
conds = [
    df.rider_id == trips_df.trips_rider_id,
    df.month == trips_df.trips_month,
    df.year == trips_df.trips_year,
]
df = df.join(trips_df, conds, "inner").select(
    F.col("rider_id"),
    F.col("month"),
    F.col("year"),
    F.col("sum_amount"),
    F.col("sum_time_spent"),
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
