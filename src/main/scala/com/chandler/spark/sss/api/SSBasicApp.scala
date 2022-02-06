package com.chandler.spark.sss.api

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType

/**
 *
 * Spark Structured Streaming的基本操作
 * 转换操作和从Json读数据并分组
 *
 */
object SSBasicApp {
  val path = "D:\\study\\spark\\Spark-The-Definitive-Guide\\data\\test\\"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    simpleTransform(spark)
  }

  def simpleTransform(spark: SparkSession) = {
    val dataSet = spark.read.json(path)
    val dataSchema: StructType = dataSet.schema

    spark.readStream
      .schema(dataSchema)
      .json(path)
      .withColumn("stairs", expr("gt like '%stairs%' "))
//      .select("user", "model", "gt")
      .where("stairs")
      .where("gt is not null")
      .groupBy("gt")
      .count()
      .writeStream
      .outputMode("complete")
      .queryName("simple_transformation")
      .format("console")
      .start()
      .awaitTermination()
  }

  def readAndGroup(spark: SparkSession) = {
    /**
     * 将数据读出来，在内存中建立一个表 activity_counts
     */
    val dataSet = spark.read.json(path)
    val dataSchema: StructType = dataSet.schema

    spark.readStream
      .schema(dataSchema)
      .option("maxFilesPerTrigger", 1)
      .json(path)
      .groupBy("gt")
      .count()
      .writeStream
      .queryName("activity_counts")
      //      .format("memory")
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }
}
