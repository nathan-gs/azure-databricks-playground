-- Databricks notebook source
SELECT * FROM taxi_tripdata_yellow LIMIT 20

-- COMMAND ----------

SELECT vendor_name, SUM(Passenger_Count) AS TotalPassengers
FROM taxi_tripdata_yellow GROUP BY vendor_name