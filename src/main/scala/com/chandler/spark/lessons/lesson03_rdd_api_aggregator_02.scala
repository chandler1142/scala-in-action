package com.chandler.spark.lessons

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object lesson03_rdd_api_aggregator_02 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val context = new SparkContext(conf)

    val data: RDD[(String, String)] = context.parallelize(List(
      ("2", "b"),
      ("3", "c"),
      ("1", "a"),
      ("4", "d"),
      ("5", "e"),
      ("3", "c"),
      ("2", "b"),
      ("1", "a"),
      ("2", "b"),
    ))
    val resultRDD = data.groupByKey(2)
    val mappedRDD: RDD[(String, String)] = resultRDD.map(element => {
      val result: String = element._2.reduce((x, y) => x + "_" + y)
      (element._1, result)
    })

    mappedRDD.foreach(println)


    val reducedRDD: RDD[(String, String)] = data.reduceByKey((x, y) => x + "_" + y)
    reducedRDD.foreach(println)






    Thread.sleep(Int.MaxValue)


  }
}
