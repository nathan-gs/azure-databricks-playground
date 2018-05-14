// Databricks notebook source
// MAGIC %sql
// MAGIC DESCRIBE taxi_tripdata_yellow

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC In Machine Learning we often itterate many times over our datasets. Let's make sure the DBIO cache is enabled.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ```
// MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "true")
// MAGIC spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "50g")
// MAGIC spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "2g")
// MAGIC ```

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from taxi_tripdata_yellow LIMIT 10

// COMMAND ----------

// MAGIC %md
// MAGIC ## Extracting features

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Let's start by extracting the pickup hour & day of week 

// COMMAND ----------

import org.apache.spark.ml.feature.SQLTransformer

val timeAndDateTransformer = new SQLTransformer()
  .setStatement("SELECT *, HOUR(PickupDateTime) AS PickupHour, DAYOFWEEK(PickupDateTime) as PickupDayOfWeek FROM __THIS__")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### String's need some special attention
// MAGIC 
// MAGIC For `RateCode` and `PaymentType` we are going to use a `StringIndexer`. A `StringIndexer` maps the string values to a numeric set.

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct RateCode from taxi_tripdata_yellow

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

val rateCodeIndexer = new StringIndexer()
  .setHandleInvalid("skip")
  .setInputCol("RateCode")
  .setOutputCol("RateCodeIndex")

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct PaymentType from taxi_tripdata_yellow

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

val paymentTypeIndexer = new StringIndexer()
  .setHandleInvalid("skip")
  .setInputCol("PaymentType")
  .setOutputCol("PaymentTypeIndex")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Combining the extracted features in a single feature column

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val assembler = new VectorAssembler()
  .setInputCols(Array("PickupHour", "PickupDayOfWeek", "FareAmount", "PassengerCount", "TripDistance", "RateCodeIndex", "PaymentTypeIndex"))
  .setOutputCol("features")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Modelling

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Split our data in Training & Test

// COMMAND ----------

val Array(training, test) = spark.sql("select * from taxi_tripdata_yellow").randomSplit(Array(0.7, 0.3))

// COMMAND ----------

training.cache()
test.cache()
println(training.count())
println(test.count())

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Create a model

// COMMAND ----------

import org.apache.spark.ml.regression.RandomForestRegressor

val rfModel = new RandomForestRegressor()
  .setLabelCol("TipAmount")
  .setFeaturesCol("features")

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegression

val lrModel = new LinearRegression()
  .setLabelCol("TipAmount")
  .setFeaturesCol("features")
  .setElasticNetParam(0.5)

println("Printing out the model Parameters:")
println("-"*20)
println(lrModel.explainParams)
println("-"*20)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Creating a pipeline

// COMMAND ----------

import org.apache.spark.ml.{Pipeline, PipelineStage}

val stepsWithoutModel:Array[PipelineStage] = Array(timeAndDateTransformer, rateCodeIndexer, paymentTypeIndexer, assembler)

val pipelineWithoutModel = new Pipeline().setStages(stepsWithoutModel)

// COMMAND ----------

display(pipelineWithoutModel.fit(training.limit(20)).transform(training.limit(10)))

// COMMAND ----------

val steps:Array[PipelineStage] = stepsWithoutModel :+ rfModel

val pipeline = new Pipeline().setStages(steps)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Applying the Pipeline & Model

// COMMAND ----------

display(training)

// COMMAND ----------

val model = pipeline.fit(training)

// COMMAND ----------


val modelTested = model.transform(test)
modelTested.createTempView("model_regression_rf_test")

display(modelTested)

// COMMAND ----------

val rfAppliedModel = model.stages.last.asInstanceOf[org.apache.spark.ml.regression.RandomForestRegressionModel]

// COMMAND ----------

rfAppliedModel.toDebugString

// COMMAND ----------

val weekDays = Seq(("Monday",2), ("Tuesday", 3), ("Wednesday", 4), ("Thursday", 5), ("Friday", 6), ("Saturday", 7), ("Sunday", 1))
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("timeOfDay", "0", (0 to 23).map(_.toString), "Hour")
dbutils.widgets.dropdown("dayOfWeek", "Friday", weekDays.map(_._1), "Day Of Week")
dbutils.widgets.text("passengers", "1", "Passengers")
dbutils.widgets.text("fareAmount", "27", "Fare Amount")
dbutils.widgets.text("distance", "4.5", "Distance")
dbutils.widgets.dropdown("paymentType", "Cash", Seq("Cash", "Credit card"), "Payment Type")

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Let's see this in action

// COMMAND ----------

val hourOfDay = dbutils.widgets.get("timeOfDay").toInt
val dayOfWeek = weekDays.find(_._1 == dbutils.widgets.get("dayOfWeek")).get._2
val passengers = dbutils.widgets.get("passengers").toInt
val distance = dbutils.widgets.get("distance").toDouble
val fareAmount = dbutils.widgets.get("fareAmount").toDouble
val rateCode = "Standard rate"
val paymentType = dbutils.widgets.get("paymentType")

println(dayOfWeek)

val date = s"2018-04-$dayOfWeek $hourOfDay:00:00"

val predictionFor = model
  .transform(
    sc
    .makeRDD(Seq((date, passengers, distance, fareAmount, rateCode, paymentType)))
    .toDF("PickupDateTime", "PassengerCount", "TripDistance", "FareAmount", "RateCode", "PaymentType"))

display(predictionFor.select("PaymentType", "FareAmount", "prediction"))

// COMMAND ----------

