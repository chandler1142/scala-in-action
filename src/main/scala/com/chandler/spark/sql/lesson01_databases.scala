package com.chandler.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalog.{Database, Table}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, catalog}

object lesson01_databases {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("app")
      .master("local[2]")
//      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    //看看catalog
    val databases: Dataset[Database] = spark.catalog.listDatabases()
    databases.show()

    val tables: Dataset[Table] = spark.catalog.listTables()
    tables.show()

    val functions: Dataset[catalog.Function] = spark.catalog.listFunctions()
    functions.show(numRows = 999, truncate = false)

    println("--------------------------------------------")

    val df: DataFrame = spark.read.format("json").json("data/emp.txt")
    df.printSchema()
    df.show()

    df.createTempView("chandler_table")

    //出现注册了表
    import scala.io.StdIn._
    spark.catalog.listTables().show()
    while (true) {
      val sql: String = readLine("input your sql: ")
      spark.sql(sql).show()
    }


    spark.stop()


  }
}
