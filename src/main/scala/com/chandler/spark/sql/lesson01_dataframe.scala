package com.chandler.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object lesson01_dataframe {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("app")
      .master("local[2]")
      //      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    //数据+元数据 = df
    //1. 数据： RDD[Row]
    val rdd: RDD[String] = sc.textFile("./data/person.txt")

    val values: RDD[Row] = rdd.map(line => {
      val strs: Array[String] = line.split(" ")
      (strs(0), strs(1).toInt)
    }).map(x => Row(x._1, x._2))

    //2. 元数据: StructType
    val schema = StructType.apply(Seq(
      StructField("name", StringType, false),
      StructField("age", IntegerType, false),
    ))

    //3. 创建df
    val dataFrame: DataFrame = spark.createDataFrame(values, schema)

    dataFrame.show()
    dataFrame.printSchema()
    dataFrame.createTempView("person")
    spark.sql("select * from person").show()

    //spark 实现了catalog 只做编目，不做语法解析和DDL，因为想和hive metastore 结合
    //以下语句会报错
    spark.sql("create table ooxx(name string, age int)")
    spark.catalog.listTables().show()





    spark.close()
  }
}
