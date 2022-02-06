package com.chandler.spark.sss.api

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object lesson21_UDFApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    udfTest02(spark)
  }

  def udfTest01(spark: SparkSession) = {
    val udfExampleDF = spark.range(5).toDF("num")
    udfExampleDF.show()

    def power3(number: Double): Double = number * number * number

    //注册到spark api
    val power3UDF = udf(power3(_: Double): Double)

    udfExampleDF.select(power3UDF(col("num"))).show()

  }

  def udfTest02(spark: SparkSession) = {
    val udfExampleDF = spark.range(5).toDF("num")
    udfExampleDF.createOrReplaceTempView("Numbers")

    def power3(number: Double): Double = number * number * number
    //注册到spark sql
    val power3UDF = spark.udf.register("power3", power3(_: Double): Double)

    spark.sql(
      """
        |select
        |power3(*)
        |from Numbers
        |""".stripMargin)
      .show()


  }
}