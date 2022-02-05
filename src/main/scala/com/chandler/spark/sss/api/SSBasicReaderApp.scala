package com.chandler.spark.sss.api

import org.apache.spark.sql.SparkSession

object SSBasicReaderApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
    spark.sql("select * from activity_counts").show()
  }
}
