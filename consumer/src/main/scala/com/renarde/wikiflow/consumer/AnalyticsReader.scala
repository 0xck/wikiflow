package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}


object AnalyticsReader extends App with LazyLogging {

  val appName: String = "analytics-reader-example"

  val spark: SparkSession = SparkSession.builder ()
    .appName (appName)
    .config ("spark.driver.memory", "5g")
    .master ("local[2]")
    .getOrCreate ()

  spark.sparkContext.setLogLevel ("WARN")

  logger.info ("Initializing Analytics Reader")

  val inputStream: DataFrame = spark.readStream
    .parquet ("/storage/analytics-consumer/output")

  val consoleOutput = inputStream.writeStream
    .outputMode ("append")
    .format ("console")
    .start ()

  spark.streams.awaitAnyTermination ()

}
