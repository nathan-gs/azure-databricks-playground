-- Databricks notebook source
-- MAGIC %md Because we will be using the table on many occassions let's cache it in-memory

-- COMMAND ----------

CACHE TABLE taxi_tripdata_yellow 