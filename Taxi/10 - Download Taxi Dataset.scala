// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC We are going to download the TLC Taxi dataset.
// MAGIC More details can be found here:
// MAGIC 
// MAGIC http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Since the TLC Yellow Cab dataset is available from 2009 till 2017, let's do some downloads.

// COMMAND ----------

val years = 2009 to 2017
val months = (1 to 12).map(m => f"${m}%02d")

val yearAndMonth = years.map(year => months.map(month => (year, month)))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We are going to download the dataset, leveraging Spark to distribute the download process. The downloads will happen on all worker nodes (distributed). Always be careful not to DDOS your own systems.
// MAGIC 
// MAGIC To do the actual download I leverage curl, spawned from a shell. 

// COMMAND ----------

sc
  .parellize(yearAndMonth) // parallelize allows you to distribute a local datastructure to a RDD.
  .foreach{case (year, month) => {
    import sys.process._
    s"mkdir -p /dbfs/mnt/data/taxi/tripdata/yellow/year=$year/month=$monthPrefixed/" !! 
    
    s"curl -o /dbfs/mnt/data/taxi/tripdata/yellow/year=$year/month=$monthPrefixed/yellow-$year-$monthPrefixed.csv https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_$year-$monthPrefixed.csv" !!
    
  }}

// COMMAND ----------

// MAGIC %sh
// MAGIC ls -R /dbfs/mnt/data/