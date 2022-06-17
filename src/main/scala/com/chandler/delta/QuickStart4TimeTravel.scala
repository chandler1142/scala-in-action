package com.chandler.delta

import org.apache.spark.sql.SparkSession

import java.io.File

object QuickStart4TimeTravel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")

    import spark.implicits._

    val path = new File("/tmp/delta-streaming/delta-table").getAbsolutePath
    val history = spark.sql("show tables " )

    println(history)

    spark.stop()
  }
}
