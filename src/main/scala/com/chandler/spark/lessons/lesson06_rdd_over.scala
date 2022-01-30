package com.chandler.spark.lessons

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object lesson06_rdd_over {
  def main(args: Array[String]): Unit = {
    //综合应用算子
    //topN 分组取topN (二次排序)
    //同月份，温度最高的2天

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("topN")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val file: RDD[String] = sc.textFile("data/data.txt")
    //2019-6-1 39
    val data = file.map(line => line.split("\t")).map(arr => {
      val arrs: Array[String] = arr(0).split("-")
      (arrs(0).toInt, arrs(1).toInt, arrs(2).toInt, arr(1).toInt)
    })

    data.sortBy(t4=>(t4._1, t4._2, t4._3))


    //分组，取topN, groupByKey有潜在风险，如果数据很大的情况下
    //    val grouped = data.map(t4 => {
    //      ((t4._1, t4._2), (t4._3, t4._4))
    //    }).groupByKey()
    //    val topN: RDD[((Int, Int), List[(Int, Int)])] = grouped.mapValues(arr => {
    //      val map = new mutable.HashMap[Int, Int]()
    //      arr.foreach(e => {
    //        if (map.get(e._1).getOrElse(0) < e._2)
    //          map.put(e._1, e._2)
    //      })
    //      map.toList.sorted(new Ordering[(Int, Int)] {
    //        override def compare(x: (Int, Int), y: (Int, Int)): Int = {
    //          y._2.compareTo(x._2)
    //        }
    //      })
    //    })
    //    topN.foreach(println)

    implicit val descOrder = new Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)): Int = {
        y._2.compareTo(x._2)
      }
    }

    val reduced: RDD[((Int, Int, Int), Int)] = data.map(t4 => ((t4._1, t4._2, t4._3), t4._4)).reduceByKey((x: Int, y: Int) => if (y > x) y else x)
    val mapped: RDD[((Int, Int), (Int, Int))] = reduced.map(t2 => ((t2._1._1, t2._1._2), (t2._1._3, t2._2)))
    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = mapped.groupByKey()
    grouped.mapValues(arr => arr.toList.sorted).foreach(println)

  }
}
