package com.chandler.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    //单词统计
    //Dataset
    val fileRDD: RDD[String] = sc.textFile("data/words.txt")
    //hello world
    //    val result = fileRDD.flatMap((x) => x.split(" "))
    //      .map((_, 1))
    //      .groupBy(_._1)
    //      .mapValues(_.size)

    val result = fileRDD
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _) //(x, y) x是oldValue y是value
    result.foreach(println)


  }
}
