# Databricks notebook source
from silver import *
from pyspark.sql.functions import months_between, col

SILVER_TABLE = "divvy.fact_trips"

spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE};")

riders_df = spark.table("divvy.dim_riders").select(
    col("rider_id").alias("rider_rider_id"),
    col("birthday"),
)
trips_df = spark.table("divvy.bronze_trips")

trips_df = trips_df.withColumn("start_at", trips_df.start_at.cast("timestamp"))
trips_df = trips_df.withColumn("ended_at", trips_df.ended_at.cast("timestamp"))
trips_df = trips_df.withColumn("start_at_date", trips_df.start_at.cast("date"))
trips_df = trips_df.withColumn("ended_at_date", trips_df.ended_at.cast("date"))
trips_df = trips_df.withColumn("rider_id", trips_df.rider_id.cast("int"))
trips_df = trips_df.withColumn(
    "time_spent", trips_df.ended_at.cast("long") - trips_df.start_at.cast("long")
)

joined_df = trips_df.join(
    riders_df, trips_df.rider_id == riders_df.rider_rider_id, "inner"
)
joined_df = joined_df.withColumn(
    "rider_age_at_time",
    (months_between(trips_df.start_at, joined_df.birthday) / 12).cast("int"),
)
joined_df.drop(
    "rider_rider_id",
)

joined_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
