package com.chandler.spark.sss

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object EventTimeWindowApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    eventTimeWindowApp(spark)
  }


  def eventTimeWindowApp(spark: SparkSession) = {
    val schema = new StructType()
      .add("ts", TimestampType)
      .add("type", StringType)

    spark.readStream
      .format("csv")
      .schema(schema)
      .load("data/event")
      .groupBy(
        window(col("ts"), "10 minutes", "5 minutes"),
        col("type")
      ).count().sort(col("window"))
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

}
