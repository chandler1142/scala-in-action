package com.chandler.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}

object lesson02_sql_api01 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val dataDF: DataFrame = List(
      "hello world",
      "hello spark",
      "hello chandler",
    ).toDF("line")

    //注册到catalog里面
    dataDF.createTempView("ctable")

    val df: DataFrame = spark.sql("select * from ctable")
    df.show()
    df.printSchema()

    println("-----------------------------------------------------")
    spark.sql("select count(*), word from (select explode(split(line,' ')) as word from ctable) as tt group by word").show()

    println("-----------------------------------------------------")
    //面向API的时候，df相当于from table
    val subTable: DataFrame = dataDF.selectExpr("explode(split(line, ' ')) as word")
    val dataset: RelationalGroupedDataset = subTable.groupBy("word")
    val res: DataFrame = dataset.count()

    res.show()
    res.write.parquet("bigdata-spark/data/out/sql-api")
  }
}
