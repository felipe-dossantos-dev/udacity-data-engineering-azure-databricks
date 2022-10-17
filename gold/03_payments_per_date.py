# Databricks notebook source
from silver import *
from pyspark.sql.functions import sum, avg, col


GOLD_TABLE = "divvy.gold_payments_per_date"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_payments")
date_df = spark.table("divvy.dim_date")

df = df.join(date_df, df.date == date_df.date, "inner")

# Analyze how much money is spent Per month, quarter, year
df = (
    df.groupby(df.year, df.quarter_of_year, df.month)
    .agg(
        avg(df.amount).alias("avg_amount"),
        sum(df.amount).alias("sum_amount"),
    )
    .orderBy("year", "quarter_of_year", "month")
    .select(
        col("year"),
        col("quarter_of_year"),
        col("month"),
        col("avg_amount"),
        col("sum_amount"),
    )
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
