package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, from_json}
import StructuredConsumer.expectedSchema


object AnalyticsConsumer extends App with LazyLogging {

  val appName: String = "analytics-consumer-example"

  val spark: SparkSession = SparkSession.builder ()
    .appName (appName)
    .config ("spark.driver.memory", "5g")
    .master ("local[2]")
    .getOrCreate

  import spark.implicits._


  spark.sparkContext.setLogLevel ("WARN")

  logger.info ("Initializing Structured consumer")

  val inputStream: DataFrame = spark.readStream
    .format ("kafka")
    .option ("kafka.bootstrap.servers", "kafka:9092")
    .option ("subscribe", "wikiflow-topic")
    .option ("startingOffsets", "earliest")
    .load ()

  // please edit the code below
  val existingValues: DataFrame = inputStream
    .select ($"key" cast StringType, $"value" cast StringType)
    .where ($"value" isNotNull)

  val obtainedData: DataFrame = existingValues
    .select (from_json ($"value", expectedSchema) as "data")
    .select ("data.*")

  val countedData: DataFrame = obtainedData.select ("*")
    .where (($"bot" === false) and ($"type" =!= "142"))
    .groupBy ($"type")
    .count ()

  val transformedStream: DataFrame = countedData.withColumn ("load_dttm", current_timestamp ())

  transformedStream.writeStream
    .outputMode ("append")
    .format ("delta")
    .option ("checkpointLocation", "/storage/analytics-consumer/checkpoints")
    .start ("/storage/analytics-consumer/output")

  spark.streams.awaitAnyTermination ()
}
