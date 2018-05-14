// Databricks notebook source
// MAGIC %sql
// MAGIC DESCRIBE taxi_tripdata_yellow

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC In Machine Learning we often itterate many times over our datasets. Let's make sure the DBIO cache is enabled.

// COMMAND ----------

// MAGIC %sh
// MAGIC df -h

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

import org.apache.spark.ml.{Pipeline, PipelineStage}

val steps:Array[PipelineStage] = Array(timeAndDateTransformer, rateCodeIndexer, paymentTypeIndexer, assembler, rfModel)

val pipeline = new Pipeline().setStages(steps)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Applying the Pipeline & Model

// COMMAND ----------

display(training)

// COMMAND ----------

val pipelineFitted = pipeline.fit(training)

// COMMAND ----------

import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

import org.apache.spark.ml.evaluation.RegressionEvaluator

val paramGrid = new ParamGridBuilder()
  .addGrid(rfModel.maxDepth, Array(5, 10))
  .addGrid(rfModel.numTrees, Array(20, 60))
  .build()

val cv = new CrossValidator() // you can feel free to change the number of folds used in cross validation as well
  .setEstimator(pipeline) // the estimator can also just be an individual model rather than a pipeline
  .setEstimatorParamMaps(paramGrid)
  .setEvaluator(new RegressionEvaluator().setLabelCol("TipAmount"))

val pipelineFitted = cv.fit(training)

// COMMAND ----------

println("The Best Parameters:\n--------------------")
println(pipelineFitted.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(0))
pipelineFitted
  .bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
  .stages(0)
  .extractParamMap

// COMMAND ----------

val transformTest = pipelineFitted
  .transform(test)
  /*.selectExpr("prediction as raw_prediction", 
    "double(round(prediction)) as prediction", 
    "count", 
    """CASE double(round(prediction)) = count 
  WHEN true then 1
  ELSE 0
END as equal""")*/
display(transformTest.limit(5))