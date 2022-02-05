package com.chandler.spark.sss.sink

import org.apache.spark.sql.SparkSession

object SinkApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    sink(spark)
  }


  def sink(spark: SparkSession) = {
    import spark.implicits._

    spark.readStream.format("socket")
      .option("host", "8.130.29.166")
      .option("port", "3389")
      .load().as[String]
      .flatMap(_.split(","))
      .map((_, "chandler"))
      .toDF("word", "name")
      .writeStream
      .format("json")
      .option("path", "output")
      .option("checkpointLocation", "ckpt")
      .start().awaitTermination()
  }
}
