package com.chandler.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.beans.BeanProperty

class Person {
  @BeanProperty
  var name:String = ""
  @BeanProperty
  var age: Int = 0
}

object lesson02_dataframe_03 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("dataframe")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext
  import spark.implicits._
//    val rdd: RDD[String] = sc.textFile("data/person.txt")
//
//    val rddBean: RDD[Person] = rdd.map(_.split(" "))
//      .map(arr => {
//        val p = new Person
//        p.setName(arr(0))
//        p.setAge(arr(1).toInt)
//        p
//      })
//
//    val df: DataFrame = spark.createDataFrame(rddBean, classOf[Person])
//
//    df.show()
//    df.printSchema()
  }



}
