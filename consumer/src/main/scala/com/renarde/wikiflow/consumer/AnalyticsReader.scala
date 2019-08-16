package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


object AnalyticsReader extends App with LazyLogging {

  val appName: String = "analytics-reader-example"

  val spark: SparkSession = SparkSession.builder ()
    .appName (appName)
    .config ("spark.driver.memory", "5g")
    .master ("local[2]")
    .getOrCreate ()

  import spark.implicits._


  spark.sparkContext.setLogLevel ("WARN")

  logger.info ("Initializing Analytics Reader")

  val schema: StructType = spark.read.parquet ("/storage/analytics-consumer/output").schema

  val inputStream: DataFrame = spark.readStream
    .schema (schema)
    .parquet ("/storage/analytics-consumer/output")

  val data: DataFrame = inputStream
    .select ($"type", $"count", $"window" as "origin_time_range", $"load_dttm")

  val consoleOutput: StreamingQuery = data.writeStream
    .outputMode ("append")
    .format ("console")
    .start ()

  spark.streams.awaitAnyTermination ()
}
