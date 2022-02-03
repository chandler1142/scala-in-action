package com.chandler.spark.sql

import org.apache.spark.sql.SparkSession

object lesson01_app {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("app")
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.format("json").load("data/emp.txt")
    df.printSchema()
    df.show()


    //sql
    df.createOrReplaceTempView("emp")
    spark.sql(
      """
        |select
        |name, age
        |from
        |emp
        |where age > 30
        |""".stripMargin).show(false)

    spark.stop()
  }
}
