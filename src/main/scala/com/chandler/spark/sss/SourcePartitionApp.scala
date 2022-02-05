package com.chandler.spark.sss

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object SourcePartitionApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]")
      .appName(this.getClass.getName).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    readCSV(spark)

  }

  def readCSV(spark: SparkSession) = {
    val userSchema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("age", IntegerType)
      .add("score", IntegerType)
    spark.readStream
      .format("csv")
      .schema(userSchema)
      .load("data/partition")
      .writeStream
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()
  }
}
