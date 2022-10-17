# CREATE TABLE [dbo].[fact_trip]
# (
#     [trip_id] nvarchar(400) null,
#     [time_spent] int null,
#     [rideable_type] nvarchar(50) null,
#     [rider_age_at_time] int null,
#     [rider_id] bigint null,
#     [start_station_id] nvarchar(400) null,
#     [start_at_date] date,
#     [start_at_time] time,
#     [start_at_time_key] time,
#     [end_station_id] nvarchar(400) null,
#     [ended_at_date] date,
#     [ended_at_time] time,
#     [ended_at_time_key] time,
#     [is_member] bit
# )
# SELECT
#     [st].trip_id,
#     DATEDIFF(MINUTE, [st].start_at, [st].ended_at),
#     [st].rideable_type,
#     DATEDIFF(YEAR, [dr].birthday, [st].start_at),
#     [st].rider_id,
#     [st].start_station_id,
#     CONVERT(date, [st].start_at),
#     CONVERT(time, [st].start_at),
#     CONVERT(time, DATEADD(MINUTE, DATEDIFF(MINUTE, 0, [st].start_at), 0)),
#     [st].end_station_id,
#     CONVERT(date, [st].ended_at),
#     CONVERT(time, [st].ended_at),
#     CONVERT(time, DATEADD(MINUTE, DATEDIFF(MINUTE, 0, [st].ended_at), 0)),
#     [dr].is_member
# FROM
#   [dbo].[staging_trip] AS [st]
# INNER JOIN [dbo].[dim_rider] AS dr ON [st].rider_id = [dr].rider_id;
from silver import *
from pyspark.sql.functions import months_between, col

SILVER_TABLE = "divvy.fact_trip"

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
    "time_spent", trips_df.start_at.cast("long") - trips_df.ended_at.cast("long")
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
