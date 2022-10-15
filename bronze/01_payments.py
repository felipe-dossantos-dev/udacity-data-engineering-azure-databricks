from bronze import *

BRONZE_TABLE = "divvy.bronze_payments"

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE};")
df = (
    spark.read.format("csv").option("sep", ",").load("/FileStore/raw_data/payments.csv")
)
df = df.withColumn("payment_id", df._c0.cast("int"))
df = df.withColumn("date", df._c1.cast("date"))
df = df.withColumn("amount", df._c2.cast("decimal"))
df = df.withColumn("rider_id", df._c3.cast("int"))

columns_to_drop = ["_c0", "_c1", "_c2", "_c3"]
df = df.drop(*columns_to_drop)

df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)
