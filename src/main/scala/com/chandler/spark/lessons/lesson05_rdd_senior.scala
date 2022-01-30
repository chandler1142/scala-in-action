package com.chandler.spark.lessons

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object lesson05_rdd_senior {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local")
      .setAppName("Test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data: RDD[Int] = sc.parallelize(1 to 10, 2)
    data.sample(false, 0.1, 222)

    println(s"data: ${data.getNumPartitions}")

    val data1: RDD[(Int, Int)] = data.mapPartitionsWithIndex(
      (pi, pt) => {
        pt.map(e => (pi, e))
      }
    )
    data1.foreach(println)

    val repartition = data1.repartition(4)

    val res = repartition.mapPartitionsWithIndex(
      (pi, pt) => {
        pt.map(e => (pi, e))
      }
    )

    println(s"res: ${res.getNumPartitions}")
    res.foreach(println)


  }


}
