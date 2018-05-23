// Databricks notebook source
display(spark.sql("select * from taxi_tripdata_yellow_csv LIMIT 5"))


// COMMAND ----------

// MAGIC %sql
// MAGIC REFRESH TABLE rate_codes;
// MAGIC REFRESH TABLE payment_types;
// MAGIC REFRESH TABLE vendors;
// MAGIC REFRESH TABLE taxi_tripdata_yellow_csv;

// COMMAND ----------

val csvDS = spark.sql("select * from taxi_tripdata_yellow_csv")
csvDS
  .write
  .mode("overwrite")
  .option("path", "/mnt/data/tables/tripdata_yellow")
  .format("parquet")
  .partitionBy("year", "month")
  .saveAsTable("taxi_tripdata_yellow")
  

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/data/tables/taxi_tripdata_yellow/