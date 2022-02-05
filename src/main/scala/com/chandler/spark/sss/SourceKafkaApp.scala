package com.chandler.spark.sss

import org.apache.spark.sql.SparkSession

object SourceKafkaApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]")
      .appName(this.getClass.getName)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    kafkaSource(spark)
  }

  def kafkaSource(spark: SparkSession) = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "access-log-prod")
      .option("includeHeaders", "true")
      .load()
//      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
  }
}
