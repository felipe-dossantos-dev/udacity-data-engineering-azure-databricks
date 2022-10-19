# Databricks notebook source
from gold import *

GOLD_TABLE = "divvy.gold_payments_per_rides"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_payments")
date_df = spark.table("divvy.dim_date").select(
    F.col("date"),
    F.col("month"),
)
df = df.join(date_df, df.date == date_df.date, "inner")


# Analyze how much money is spent per member Based on how many rides the rider averages per month
df = (
    df.groupby(df.rider_id, df.month)
    .agg(
        F.avg(df.amount).alias("avg_amount"),
        F.sum(df.amount).alias("sum_amount"),
    )
    .orderBy("rider_id", "month")
    .select(
        F.col("rider_id"),
        F.col("month"),
        F.col("avg_amount"),
        F.col("sum_amount"),
    )
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
