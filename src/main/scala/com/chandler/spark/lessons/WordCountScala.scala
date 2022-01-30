package com.chandler.spark.lessons

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    //单词统计d，统计单词出现为2次的单词个数
    //Dataset
    //返回的是HadoopRDD
    val fileRDD: RDD[String] = sc.textFile("data/words.txt", 1)

    val res = fileRDD
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _) //(x, y) x是oldValue y是value

    /**
     * (hello, 2)  ==> (2, 1)
     * (msb, 1) ==> (1, 1)
     * (world, 2) ==> (2, 1)
     */
    val res2 = res.map((x) => {
      (x._2, 1)
    }).reduceByKey(_ + _)

    res.foreach(println)
    res2.foreach(println)

    //访问localhost:4040 查看Job task
    Thread.sleep(Long.MaxValue)
  }
}
