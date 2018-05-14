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

val yearAndMonth = years
  .map(year => months.map(month => (year, month)))
  .flatten

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We are going to download the dataset, leveraging Spark to distribute the download process. The downloads will happen on all worker nodes (distributed). Always be careful not to DDOS your own systems.
// MAGIC 
// MAGIC To do the actual download I leverage curl, spawned from a shell. 

// COMMAND ----------

sc
  .parallelize(yearAndMonth) // parallelize allows you to distribute a local datastructure to a RDD.
  .foreach{case (year, month) => {
    import sys.process._
    s"mkdir -p /dbfs/mnt/data/taxi/tripdata/yellow/year=$year/month=$month/" !! 
    
    
    s"curl -o /dbfs/mnt/data/taxi/tripdata/yellow/year=$year/month=$month/yellow-$year-$month.csv https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_$year-$month.csv" !!        
    
  }}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Let's quickly take a look at the directory structure.

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/data/taxi/tripdata/yellow/

// COMMAND ----------

// MAGIC %md
// MAGIC We use a structure of `year=2017/month=02` because Spark (& Hive) interpretes this as a `virtual` column. This allows us to quickly only select part of the dataset, without having to go through all of your data.

// COMMAND ----------

// MAGIC %md ##Download some reference data

// COMMAND ----------

// MAGIC %sh
// MAGIC mkdir -p /dbfs/mnt/data/taxi/refdata/

// COMMAND ----------

// MAGIC %sh
// MAGIC curl -o /dbfs/mnt/data/taxi/refdata/taxizonelookup.csv "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv" 
// MAGIC curl -o /dbfs/mnt/data/taxi/refdata/taxizones.zip "https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip"