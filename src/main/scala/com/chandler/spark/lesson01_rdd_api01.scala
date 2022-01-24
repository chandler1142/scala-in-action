package com.chandler.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object lesson01_rdd_api01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test01")
    val sc = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6))

    //    dataRDD.map()
    //    dataRDD.flatMap()

    val filterRDD = dataRDD.filter(_ >= 3)
    val result = filterRDD.collect()

    result.foreach(println)
    println("--------------------------------------")

    val dataRDD1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 5, 4, 3, 2, 1))
    val res02 = dataRDD1.map((_, 1)).reduceByKey(_ + _).map(_._1)
    res02.foreach(println)

    val res03 = dataRDD1.distinct()
    res03.foreach(println)

    //map这种叫做基础API， distinct叫做复合API
    //面向数据集： 交并差， 关联， 笛卡尔积
  }

}
