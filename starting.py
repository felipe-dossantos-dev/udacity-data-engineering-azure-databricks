# Databricks notebook source
df = spark.read.format("csv") \
  .option("sep", ",") \
  .load("/FileStore/raw_data/payments.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("/delta/bronze_payments")

# COMMAND ----------

bronze_df = df = spark.read.format("delta") \
  .load("/delta/bronze_payments")

display(df)

# COMMAND ----------

bronze_df = bronze_df.withColumn("payment_id",bronze_df._c0.cast("int"))
bronze_df = bronze_df.withColumn("date",bronze_df._c1.cast("date"))
bronze_df = bronze_df.withColumn("amount",bronze_df._c2.cast("decimal"))
bronze_df = bronze_df.withColumn("rider_id",bronze_df._c3.cast("int"))

columns_to_drop = ['_c0', '_c1', '_c2', '_c3']
bronze_df = bronze_df.drop(*columns_to_drop)

bronze_df.write.format("delta").mode("overwrite") \
.saveAsTable("default.silver_payments")

# COMMAND ----------


