# Databricks notebook source
from silver import *
from pyspark.sql.functions import sum, avg, col


GOLD_TABLE = "divvy.gold_payments_per_age"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_payments")
# Analyze how much money is spent Per member, based on the age of the rider at account start
# rider_age_account_start
df = (
    df.groupby(df.rider_age_account_start)
    .agg(
        avg(df.amount).alias("avg_amount"),
        sum(df.amount).alias("sum_amount"),
    )
    .orderBy(df.rider_age_account_start)
    .select(
        col("rider_age_account_start"),
        col("avg_amount"),
        col("sum_amount"),
    )
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
