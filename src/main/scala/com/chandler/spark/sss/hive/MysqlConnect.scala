package com.chandler.spark.sss.hive

import org.apache.spark.sql.SparkSession

object MysqlConnect {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    spark.sql("use test_db")
    spark.sql("show tables").show(10)

    spark.close()
  }
}
