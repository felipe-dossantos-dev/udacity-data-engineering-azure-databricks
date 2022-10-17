# Databricks notebook source
from silver import *
from pyspark.sql.functions import months_between, col

SILVER_TABLE = "divvy.fact_payments"

spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE};")

riders_df = spark.table("divvy.dim_riders").select(
    col("rider_id").alias("rider_rider_id"), col("birthday"), col("account_start_date")
)
payments_df = spark.table("divvy.bronze_payments")

payments_df = payments_df.withColumn("payment_id", payments_df.payment_id.cast("int"))
payments_df = payments_df.withColumn("date", payments_df.date.cast("date"))
payments_df = payments_df.withColumn("amount", payments_df.amount.cast("decimal"))
payments_df = payments_df.withColumn("rider_id", payments_df.rider_id.cast("int"))

joined_df = payments_df.join(
    riders_df, payments_df.rider_id == riders_df.rider_rider_id, "inner"
)
joined_df = joined_df.withColumn(
    "rider_age_account_start",
    (months_between(joined_df.account_start_date, joined_df.birthday) / 12).cast("int"),
)
joined_df.drop("rider_rider_id", "birthday", "account_start_date")

joined_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
