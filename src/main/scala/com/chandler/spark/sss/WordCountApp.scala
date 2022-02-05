package com.chandler.spark.sss

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WordCountApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName).getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val lines = spark.readStream.format("socket")
      .option("host", "8.130.29.166")
      .option("port", 3389)
      .load()

    val wordCount: DataFrame =
      lines.as[String]
        .flatMap(_.split(","))
        .groupBy("value")
        .count()

    wordCount.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()

  }
}
