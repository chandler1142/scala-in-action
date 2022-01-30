package com.chandler.spark.lessons

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object lesson03_rdd_aggregator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("aggregator")
    val sc: SparkContext = new SparkContext(conf)

    val data: RDD[(String, Int)] = sc.parallelize(List(
      ("zhangsan", 234),
      ("zhangsan", 456),
      ("zhangsan", 123),
      ("lisi", 222),
      ("lisi", 111),
      ("lisi", 333),
      ("wangwu", 777),
      ("wangwu", 666),
    ))

    //key value -> 一组
    val group: RDD[(String, Iterable[Int])] = data.groupByKey()
    group.foreach(println)

    //行列转换
    val res01: RDD[(String, Int)] = group.flatMap(e => e._2.map((e._1, _)))
    res01.foreach(println)
    println("---------------------------------")

    val res02 = group.flatMapValues(_.iterator)
    res02.foreach(println)
    println("---------------------------------")

    val newGroup: RDD[(String, List[Int])] = group.mapValues(e => e.toList.sorted.take(2))
    newGroup.foreach(println)
    println("---------------------------------")

    newGroup.flatMapValues(e => e.iterator).foreach(println)
    println("---------------------------------")


    println("----- sum, count, min, max, avg ------")

    val sum: RDD[(String, Int)] = data.reduceByKey(_ + _)
    sum.foreach(println)

    val max: RDD[(String, Int)] = data.reduceByKey((ov, nv) => if (ov > nv) ov else nv)
    val min: RDD[(String, Int)] = data.reduceByKey((ov, nv) => if (ov < nv) ov else nv)
    val count: RDD[(String, Int)] = data.mapValues(e => 1).reduceByKey(_ + _)

    val tmp: RDD[(String, (Int, Int))] = sum.join(count)
    val avg: RDD[(String, Int)] = tmp.mapValues(e => e._1 / e._2)

    sum.foreach(println)
    avg.foreach(println)


    println("-----------------avg combine--------------------------")
    val tmpx: RDD[(String, (Int, Int))] = data.combineByKey(
      //createCombiner: V=>C
      (value: Int) => (value, 1),
      //mergeValue: (C, V) => C,
      //如果又第二条记录，第二条以及以后的 他们的value怎么放到hashmap里
      (oldValue: (Int, Int), newValue: Int) => (oldValue._1 + newValue, oldValue._2 + 1),
      //mergeCombiners: (C,C) => C,
      //合并溢写,即合并各个合并器得出最终的结果
      (v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2)
    )
    tmpx.mapValues(e => e._1 / e._2).foreach(println)

    Thread.sleep(Long.MaxValue)
  }
}
