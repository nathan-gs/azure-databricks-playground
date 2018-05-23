-- Databricks notebook source
DESCRIBE taxi_tripdata_yellow

-- COMMAND ----------

SELECT PaymentType, COUNT(*) 
FROM taxi_tripdata_yellow 
GROUP BY PaymentType

-- COMMAND ----------

SELECT Year, Month, COUNT(*) 
FROM taxi_tripdata_yellow 
WHERE Year < 2016
GROUP BY Year, Month
ORDER BY Year, Month

-- COMMAND ----------

SELECT PassengerCount, COUNT(*) 
FROM taxi_tripdata_yellow 
GROUP BY PassengerCount
ORDER BY PassengerCount DESC

-- COMMAND ----------

select * from (
select 
ROUND(PickupLongitude, 4), 
ROUND(PickupLatitude, 4),
count(*) as c
from taxi_tripdata_yellow 
WHERE 
PickupLongitude IS NOT NULL
AND
PickupLatitude IS NOT NULL
AND
year = 2014
AND
month = 2
AND
ROUND(PickupLongitude, 4) BETWEEN -74.259090 AND -73.9
AND
ROUND(PickupLatitude, 4) BETWEEN 40.477399 AND 40.917577
GROUP BY 
ROUND(PickupLongitude, 4), 
ROUND(PickupLatitude, 4)
  ) 
  WHERE c > 5

-- COMMAND ----------

SELECT AVG(FareAmount / TripDistance)
FROM taxi_tripdata_yellow

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val html = "https://msit.powerbi.com/view?r=eyJrIjoiMTk1OWMyMjgtMTgyZC00MGQ4LWEwYjEtZWQyNzI1N2FmNmMzIiwidCI6IjcyZjk4OGJmLTg2ZjEtNDFhZi05MWFiLTJkN2NkMDExZGI0NyIsImMiOjV9"
-- MAGIC displayHTML(html)