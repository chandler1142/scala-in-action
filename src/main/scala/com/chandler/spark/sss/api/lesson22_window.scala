package com.chandler.spark.sss.api

import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.{DataFrame, SparkSession}

object lesson22_window {
  def main(args: Array[String]): Unit = {
    val path = "D:\\study\\spark\\Spark-The-Definitive-Guide\\data\\test\\"
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val dataSet = spark.read.json(path)

    val streaming: DataFrame = spark
      .readStream
      .schema(dataSet.schema)
      .option("maxFilesPerTrigger", 2)
      .json(path)

    /**
     *
     * root
     * |-- Arrival_Time: long (nullable = true)
     * |-- Creation_Time: long (nullable = true)
     * |-- Device: string (nullable = true)
     * |-- Index: long (nullable = true)
     * |-- Model: string (nullable = true)
     * |-- User: string (nullable = true)
     * |-- gt: string (nullable = true)
     * |-- x: double (nullable = true)
     * |-- y: double (nullable = true)
     * |-- z: double (nullable = true)
     */
    println(streaming.printSchema())

    val withEventTime = streaming.selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

    //滚动窗口 window
    withEventTime
      //增加水位线，舍弃过期数据
      .withWatermark("event_time", "5 seconds")
      //删除重复项
      .dropDuplicates("User", "event_time")
      .groupBy(window(col("event_time"), "10 minutes")).count()
      .sort(col("window"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }

}
