package com.chandler.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object lesson04_sql_standalone_hive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.sql.warehouse.dir", "D:\\study\\scala-in-action\\output\\spark-warehouse")
      .enableHiveSupport() //开启Hive支持 自己会启动hive的metastore
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("INFO")

    import spark.sql
//    sql("create database chandler_db")
//    spark.sql("create table chandler_db.table01(name string, age int)")
//
//    spark.sql("insert into chandler_db.table01 values ('chandler', 18)")
//

    spark.catalog.listTables().show()
    sql("create database db01")
    sql("create table table01(name string)") //作用在当前current 库
    spark.catalog.listTables().show() //作用在current 库
    println("-----------------------------------------")
    sql("use db01")
    spark.catalog.listTables().show() //作用在db01 库

    sql("create table table02(name string)") //作用在当前current 库
    spark.catalog.listTables().show() //作用在db01 库



    //    spark.sql("select * from chandler_db.table01").show()

  }
}
