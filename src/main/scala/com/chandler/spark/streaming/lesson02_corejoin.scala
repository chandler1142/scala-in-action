package com.chandler.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object lesson02_corejoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkStreamingWordCount")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)


    val list = new ListBuffer[(String, Boolean)]
    list.append(("name", true))
    val listRDD = sc.parallelize(list)

    val input = new ListBuffer[(String, String)]()
    input.append(("name", "20221212.123"))
    input.append(("test", "20221212.234"))
    val inputRDD = sc.parallelize(input)

    val filterRDD = inputRDD.leftOuterJoin(listRDD)

    filterRDD.foreach(println)
    filterRDD.filter(x => {
      x._2._2.getOrElse(false) != true
    }).foreach(println)

    sc.stop()
  }
}
