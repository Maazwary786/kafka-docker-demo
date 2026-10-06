package com.shipment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ShipmentStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RealTimeShipmentTracking")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schema = new StructType()
      .add("shipment_id", StringType)
      .add("location", StringType)
      .add("status", StringType)
      .add("latitude", DoubleType)
      .add("longitude", DoubleType)
      .add("event_time", LongType)

    val events = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "shipment-events")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")
      .withColumn("event_timestamp", (col("event_time") / 1000).cast("timestamp"))

    val result = events
      .withWatermark("event_timestamp", "10 minutes")
      .groupBy(window(col("event_timestamp"), "30 seconds"), col("status"))
      .count()

    val query = result.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "logs/checkpoint")
      .start()

    println("Shipment streaming job started. Waiting for Kafka events...")
    query.awaitTermination()
  }
}
