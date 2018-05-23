// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Regression: Applying a previously trained model
// MAGIC 
// MAGIC During the previous step we created a `mllib` model. During this phase we are going to apply it. 

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

// MAGIC %sql
// MAGIC select * from taxi_tripdata_yellow LIMIT 10

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Loading a previously trained model

// COMMAND ----------

import org.apache.spark.ml.PipelineModel
//val model = pipeline.fit(training)
val model = PipelineModel.load("/mnt/data/model/taxi_regression")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Steps in model building
// MAGIC 
// MAGIC * Extracting features
// MAGIC   - Extract the `hour` of `PickupDateTime`
// MAGIC   - Extract the `weekday` of `PickupDateTime`
// MAGIC   - Extract the `passenger count`
// MAGIC   - Extract the `Fare Amount`
// MAGIC   - Extract the `rateCode`
// MAGIC     * This is a string so we need to index this. Most ML models only work with numbers.
// MAGIC   - Extract the `paymentType`
// MAGIC     * Same as `rateCode`
// MAGIC * Assembling a vector based on the extracted features
// MAGIC   - Create a new `column`, called `features` with the features converted into a `vector` with only numbers.
// MAGIC * Selecting the `column` we want to __predict__, this is called the `label`
// MAGIC * Creating a `training data` & `test data` set
// MAGIC * Training the model on the `training data` set
// MAGIC * Validating the model on the `test data` set
// MAGIC * Saving the model

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Let's see this in action

// COMMAND ----------

var toPredict: Seq[(String, Int, Double, Double, String, String)] = Seq()

// COMMAND ----------

val hourOfDay = dbutils.widgets.get("timeOfDay").toInt
val dayOfWeek = weekDays.find(_._1 == dbutils.widgets.get("dayOfWeek")).get._2
val passengers = dbutils.widgets.get("passengers").toInt
val distance = dbutils.widgets.get("distance").toDouble
val fareAmount = dbutils.widgets.get("fareAmount").toDouble
val rateCode = "Standard rate"
val paymentType = dbutils.widgets.get("paymentType")


val date = s"2018-04-$dayOfWeek $hourOfDay:00:00"

toPredict = toPredict ++ Seq((date, passengers, distance, fareAmount, rateCode, paymentType))


val predictionFor = model
  .transform(
    sc
    .makeRDD(toPredict)
    .toDF("PickupDateTime", "PassengerCount", "TripDistance", "FareAmount", "RateCode", "PaymentType"))

display(predictionFor.select("PickupHour", "PickupDayOfWeek", "PaymentType", "PassengerCount", "TripDistance", "FareAmount", "prediction"))

// COMMAND ----------

