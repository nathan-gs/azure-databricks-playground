-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC spark-csv

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC head /mnt/data/taxi/tripdata/yellow/year=2017/month=01/yellow-2017-01.csv

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC head /mnt/data/taxi/tripdata/yellow/year=2014/month=06/yellow-2014-06.csv

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC mkdir -p /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2015
-- MAGIC mv /dbfs/mnt/data/taxi/tripdata/yellow/year=20{09,10,11,12,13,14}/ /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2015

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2015

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC mkdir -p /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2016h2
-- MAGIC mv /dbfs/mnt/data/taxi/tripdata/yellow/year=2015/ /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2016h2
-- MAGIC mkdir -p /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2016h2/year=2016
-- MAGIC mv /dbfs/mnt/data/taxi/tripdata/yellow/year=2016/month={01,02,03,04,05,06} /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2016h2/year=2016/

-- COMMAND ----------

-- MAGIC %sh 
-- MAGIC ls -R /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2016h2/

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC mkdir -p /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2017
-- MAGIC mv /dbfs/mnt/data/taxi/tripdata/yellow/year=2016/ /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2017

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC mkdir -p /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2018
-- MAGIC mv /dbfs/mnt/data/taxi/tripdata/yellow/year=2017/ /dbfs/mnt/data/taxi/tripdata/yellow/yellow_pre2018

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC sync

-- COMMAND ----------

DROP TABLE tmp_taxi_tripdata_yellow

-- COMMAND ----------


CREATE TEMPORARY TABLE tmp_taxi_tripdata_yellow
USING csv
OPTIONS (path "/mnt/data/taxi/tripdata/yellow/yellow_pre2016h2/year=2015/month=01/yellow-2015-01.csv", header "true", inferSchema "true")

-- COMMAND ----------

describe tmp_taxi_tripdata_yellow

-- COMMAND ----------

SELECT * FROM tmp_taxi_tripdata_yellow LIMIT 20

-- COMMAND ----------

DROP TABLE taxi_tripdata_yellow_csv_pre2015; 

CREATE TABLE taxi_tripdata_yellow_csv_pre2015
USING csv
OPTIONS (path "/mnt/data/taxi/tripdata/yellow/yellow_pre2015/", header "true", infer_schema "true", mode "DROPMALFORMED")

-- COMMAND ----------

DROP TABLE taxi_tripdata_yellow_csv_pre2016h2;

CREATE TABLE taxi_tripdata_yellow_csv_pre2016h2
USING csv
OPTIONS (path "/mnt/data/taxi/tripdata/yellow/yellow_pre2016h2/", header "true", infer_schema "true", mode "DROPMALFORMED")

-- COMMAND ----------

DROP TABLE taxi_tripdata_yellow_csv_pre2017;

CREATE TABLE taxi_tripdata_yellow_csv_pre2017
USING csv
OPTIONS (path "/mnt/data/taxi/tripdata/yellow/yellow_pre2017/", header "true", infer_schema "true", mode "DROPMALFORMED")

-- COMMAND ----------

DROP TABLE taxi_tripdata_yellow_csv_pre2018;

CREATE TABLE taxi_tripdata_yellow_csv_pre2018
USING csv
OPTIONS (path "/mnt/data/taxi/tripdata/yellow/yellow_pre2018/", header "true", infer_schema "true", mode "DROPMALFORMED")

-- COMMAND ----------

SELECT * FROM taxi_tripdata_yellow_csv_pre2015 LIMIT 20

-- COMMAND ----------

-- MAGIC %md #Create lookup tables

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val vendorSeq = Seq(
-- MAGIC     (1, "CMT"),
-- MAGIC     (2, "Verifone"))
-- MAGIC     
-- MAGIC val vendors = sc.makeRDD(vendorSeq).toDF("VendorID", "VendorName")
-- MAGIC     
-- MAGIC vendors.write.mode("overwrite").saveAsTable("vendors")
-- MAGIC 
-- MAGIC val paymentTypeSeq = Seq(
-- MAGIC   (1, "Credit card"),
-- MAGIC   (2, "Cash"),
-- MAGIC   (3, "No charge"),
-- MAGIC   (4, "Dispute"),
-- MAGIC   (5, "Unknown"),
-- MAGIC   (6, "Voided trip"))
-- MAGIC     
-- MAGIC val paymentTypes = sc.makeRDD(paymentTypeSeq).toDF("Payment_Type", "PaymentType")
-- MAGIC     
-- MAGIC paymentTypes.write.mode("overwrite").saveAsTable("payment_types")
-- MAGIC 
-- MAGIC val rateCodeSeq = Seq(
-- MAGIC     (1, "Standard rate"),
-- MAGIC     (2, "JFK"), 
-- MAGIC     (3, "Newark"), 
-- MAGIC     (4, "Nassau or Westchester"),
-- MAGIC     (5, "Negotiated fare"), 
-- MAGIC     (6, "Group ride"),
-- MAGIC     (99, "Unknown"))
-- MAGIC     
-- MAGIC val rateCodes = sc.makeRDD(rateCodeSeq).toDF("RateCodeID", "RateCode")
-- MAGIC     
-- MAGIC rateCodes.write.mode("overwrite").saveAsTable("rate_codes")

-- COMMAND ----------

-- MAGIC %md DROP TABLE taxi_tripdata_yellow_csv;
-- MAGIC 
-- MAGIC CREATE TABLE taxi_tripdata_yellow_csv
-- MAGIC (Vendor string,
-- MAGIC  tpep_pickup_datetime timestamp,
-- MAGIC  tpep_dropoff_datetime timestamp,
-- MAGIC  passenger_count int,
-- MAGIC  trip_distance double, 
-- MAGIC  Ratecode string,
-- MAGIC  store_and_fwd_flag string,
-- MAGIC  PULocationID int,
-- MAGIC  DOLocationID int,
-- MAGIC  pickup_longitude double,
-- MAGIC  pickup_latitude double,
-- MAGIC  dropoff_longitude double,
-- MAGIC  dropoff_latitude double,
-- MAGIC  PaymentType string,
-- MAGIC  fare_amount double,
-- MAGIC  extra double, 
-- MAGIC  mta_tax double, 
-- MAGIC  tip_amount double, 
-- MAGIC  tolls_amount double,
-- MAGIC  improvement_surcharge double,
-- MAGIC  total_amount double, 
-- MAGIC  year int, 
-- MAGIC  month int
-- MAGIC )

-- COMMAND ----------

show partitions taxi_tripdata_yellow_csv_pre2015

-- COMMAND ----------

msck repair table taxi_tripdata_yellow_csv_pre2015;
msck repair table taxi_tripdata_yellow_csv_pre2016h2;
msck repair table taxi_tripdata_yellow_csv_pre2017;
msck repair table taxi_tripdata_yellow_csv_pre2018;

-- COMMAND ----------

SELECT * FROM taxi_tripdata_yellow_csv_pre2016h2 LIMIT 1

-- COMMAND ----------

CREATE TEMPORARY VIEW taxi_tripdata_yellow_csv (Vendor, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, Ratecode, store_and_fwd_flag, PULocationID, DOLocationID, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, PaymentType, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, year, month) 
AS
  SELECT t.vendor_name,
         t.Trip_Pickup_DateTime,
         t.Trip_Dropoff_DateTime,
         t.passenger_count,
         t.trip_distance,
         r.RateCode,
         t.store_and_forward,
         NULL,
         NULL,
         t.Start_Lon,
         t.Start_Lat,
         t.End_Lon,
         t.End_Lat,
         p.PaymentType,
         t.fare_amt,
         t.surcharge,
         t.mta_tax,
         t.tip_amt,
         t.tolls_amt,
         NULL,
         t.total_amt,
         t.year,
         t.month
  FROM taxi_tripdata_yellow_csv_pre2015 t
  LEFT JOIN payment_types p ON t.payment_type = p.Payment_Type
  LEFT JOIN rate_codes r ON t.rate_code = r.RateCodeID
UNION ALL
  SELECT t.VendorID,
         t.tpep_pickup_datetime,
         t.tpep_dropoff_datetime,
         t.passenger_count,
         t.trip_distance,
         r.RateCode,
         t.store_and_fwd_flag,
         NULL,
         NULL,
         t.pickup_longitude,
         t.pickup_latitude,
         t.dropoff_longitude,
         t.dropoff_latitude,
         p.PaymentType,
         t.fare_amount,
         t.extra,
         t.mta_tax,
         t.tip_amount,
         t.tolls_amount,
         t.improvement_surcharge,
         t.total_amount,
         t.year,
         t.month
  FROM taxi_tripdata_yellow_csv_pre2016h2 t
  LEFT JOIN payment_types p ON t.payment_type = p.Payment_Type
  LEFT JOIN rate_codes r ON t.RateCodeID = r.RateCodeID
UNION ALL
  SELECT v.VendorName,
         t.tpep_pickup_datetime,
         t.tpep_dropoff_datetime,
         t.passenger_count,
         t.trip_distance,
         r.RateCode,
         t.store_and_fwd_flag,
         PULocationID,
         DOLocationID,
         NULL,
         NULL,
         NULL,
         NULL,
         p.PaymentType,
         t.fare_amount,
         t.extra,
         t.mta_tax,
         t.tip_amount,
         t.tolls_amount,
         t.improvement_surcharge,
         t.total_amount,
         t.year,
         t.month
  FROM taxi_tripdata_yellow_csv_pre2017 t
  LEFT JOIN vendors v ON t.VendorID = v.VendorID
  LEFT JOIN payment_types p ON t.payment_type = p.Payment_Type
  LEFT JOIN rate_codes r ON t.RateCodeID = r.RateCodeID
UNION ALL
  SELECT v.VendorName,
         t.tpep_pickup_datetime,
         t.tpep_dropoff_datetime,
         t.passenger_count,
         t.trip_distance,
         r.RateCode,
         t.store_and_fwd_flag,
         PULocationID,
         DOLocationID,
         NULL,
         NULL,
         NULL,
         NULL,
         p.PaymentType,
         t.fare_amount,
         t.extra,
         t.mta_tax,
         t.tip_amount,
         t.tolls_amount,
         t.improvement_surcharge,
         t.total_amount,
         t.year,
         t.month
  FROM taxi_tripdata_yellow_csv_pre2018 t
  LEFT JOIN vendors v ON t.VendorID = v.VendorID
  LEFT JOIN payment_types p ON t.payment_type = p.Payment_Type
  LEFT JOIN rate_codes r ON t.RateCodeID = r.RateCodeID;


-- COMMAND ----------

SELECT Vendor, COUNT(*) 
FROM taxi_tripdata_yellow_csv
GROUP BY Vendor

-- COMMAND ----------

DESCRIBE taxi_tripdata_yellow_csv

-- COMMAND ----------

show partitions taxi_tripdata_yellow_csv_pre2018;

-- COMMAND ----------

DROP TABLE taxi_tripdata_yellow_csv