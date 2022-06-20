package com.chandler.performance

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object NameCount {
  //  def skew(spark: SparkSession, repath: String): Unit = {
  //    val db_name = Config.dbnameval sql = "select * from cyj_skew " spark
  //    .sql(s"use $db_name")
  //    val dfresult = spark.sql(sql)
  //    val rddresult = dfresult.rdd.map(x => (x.get(0), 1)).groupByKey(10) //并行度参数 = 10        .mapValues(_.size)    rddresult.saveAsTextFile(repath)  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df: DataFrame = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("data/DataSkewing.csv")

    val results = df.rdd.map(x => (x.get(3), 1)).groupByKey(10).mapValues(_.sum)

    results.foreach(println)


    println("complete")
    Thread.sleep(Int.MaxValue)
    spark.close()
  }
}
