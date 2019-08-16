package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{current_timestamp, from_json, from_unixtime, window}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object AnalyticsConsumer extends App with LazyLogging {

  val appName: String = "analytics-consumer-example"

  val spark: SparkSession = SparkSession.builder ()
    .appName (appName)
    .config ("spark.driver.memory", "5g")
    .master ("local[2]")
    .getOrCreate ()

  import spark.implicits._


  spark.sparkContext.setLogLevel ("WARN")

  logger.info ("Initializing Analytics consumer")

  val inputStream: DataFrame = spark.readStream
    .format ("kafka")
    .option ("kafka.bootstrap.servers", "kafka:9092")
    .option ("subscribe", "wikiflow-topic")
    .option ("startingOffsets", "earliest")
    .option ("failOnDataLoss", "false")
    .load ()

  // please edit the code below
  val expectedSchema = new StructType ()
    .add (StructField ("bot", BooleanType))
    .add (StructField ("comment", StringType))
    .add (StructField ("id", LongType))
    .add ("length", new StructType ()
      .add (StructField ("new", LongType))
      .add (StructField ("old", LongType)))
    .add ("meta", new StructType ()
      .add (StructField ("domain", StringType))
      .add (StructField ("dt", StringType))
      .add (StructField ("id", StringType))
      .add (StructField ("offset", LongType))
      .add (StructField ("partition", LongType))
      .add (StructField ("request_id", StringType))
      .add (StructField ("stream", StringType))
      .add (StructField ("topic", StringType))
      .add (StructField ("uri", StringType)))
    .add ("minor", BooleanType)
    .add ("namespace", LongType)
    .add ("parsedcomment", StringType)
    .add ("patrolled", BooleanType)
    .add ("revision", new StructType ()
      .add ("new", LongType)
      .add ("old", LongType))
    .add ("server_name", StringType)
    .add ("server_script_path", StringType)
    .add ("server_url", StringType)
    .add ("timestamp", LongType)
    .add ("title", StringType)
    .add ("type", StringType)
    .add ("user", StringType)
    .add ("wiki", StringType)

  val existingValues: DataFrame = inputStream
    .select ($"key" cast StringType, $"value" cast StringType)
    .where ($"value" isNotNull)

  val obtainedData: DataFrame = existingValues
    .select (from_json ($"value", expectedSchema) as "data")
    .select ("data.*")

  val cleanedData: DataFrame = obtainedData.select ("*")
    .where (($"bot" === false) and ($"type" =!= "142"))
    .withColumn ("origin_time", from_unixtime ($"timestamp") cast TimestampType)

  val transformedStream: DataFrame = cleanedData
    .withWatermark ("origin_time", "10 seconds")
    .groupBy (window ($"origin_time", "10 seconds") as "origin_time_range", $"type")
    .count ()
    .withColumn ("load_dttm", current_timestamp ())

  transformedStream.writeStream
    //    .outputMode ("append")
    .format ("delta")
    .option ("checkpointLocation", "/storage/analytics-consumer/checkpoints")
    .start ("/storage/analytics-consumer/output")

  spark.streams.awaitAnyTermination ()
}
