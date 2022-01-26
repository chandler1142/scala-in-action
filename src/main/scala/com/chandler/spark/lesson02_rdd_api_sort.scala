package com.chandler.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//问题：
//1. 为什么sort sortBy会产生一个新的job
//2. 为什么take(5)会产生两个job
object lesson02_rdd_api_sort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("sort")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val file = sc.textFile("data/pvuvdata", 5)

    //PV, UV
    //需求： 根据数据统计计算各个网站的PV,UV， 同时，只显示top5
    //解题: 按PV值，或者UV值排序，取前5名数据

    //pv:
    //208.189.214.201	江苏	2018-11-12	1542011090255	8685178694078178794	www.suning.com	Comment
    println("-------------- PV: ----------------")
    val pair = file.map(line => (line.split("\t")(5), 1))
    val reduce = pair.reduceByKey(_+_)
    val map = reduce.map((_.swap))
    val sorted: RDD[(Int, String)] = map.sortByKey(false)
    val res: RDD[(String, Int)] = sorted.map(_.swap)
    val pv: Array[(String, Int)] = res.take(5)
    pv.foreach(println)

    println("--------------UV: ----------------")
    //208.189.214.201	江苏	2018-11-12	1542011090255	8685178694078178794	www.suning.com	Comment
    val keys: RDD[(String, String)] = file.map(line => {
      val strs: Array[String] = line.split("\t")
      (strs(5), strs(0))
    })
    val key: RDD[(String, String)] = keys.distinct()
    val pairx: RDD[(String, Int)] = key.map(k=>(k._1, 1))
    val uvreduce: RDD[(String, Int)] = pairx.reduceByKey(_ + _)
    val uvSorted: RDD[(String, Int)] = uvreduce.sortBy(_._2, false)
    val uv: Array[(String, Int)] = uvSorted.take(5)
    uv.foreach(println)

    Thread.sleep(Long.MaxValue)

  }
}
