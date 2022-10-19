# Databricks notebook source
from gold import *

GOLD_TABLE = "divvy.gold_payments_per_age"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_payments")
# Analyze how much money is spent Per member, based on the age of the rider at account start
df = (
    df.groupby(df.rider_age_account_start)
    .agg(
        F.avg(df.amount).alias("avg_amount"),
        F.sum(df.amount).alias("sum_amount"),
    )
    .orderBy(df.rider_age_account_start)
    .select(
        F.col("rider_age_account_start"),
        F.col("avg_amount"),
        F.col("sum_amount"),
    )
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
