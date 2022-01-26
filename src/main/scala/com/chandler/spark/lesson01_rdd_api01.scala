package com.chandler.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//面向数据集的操作：
//1. 带函数的非聚合 : map, flatmap
//2. 单元素: union, cartesion
//3. KV元素: cogroup, join
//4. 排序
//5. 聚合计算: reduceByKey
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

    println("----------union-------------------")
    //面向数据集: 元素 ---> 单元素, KV元素, ---> 结构化、非结构化
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(List(3, 4, 5, 6, 7))
    val unionRdd = rdd1.union(rdd2)
    //打印分区和数据
    println(rdd1.partitions.size)
    println(rdd2.partitions.size)
    println(unionRdd.partitions.size)
    unionRdd.foreach(println)

    println(unionRdd.zipWithIndex())


    println("------- cartesian -------")
    //这个计算是不需要shuffle的
    //因为shuffle的语义：洗牌 ==> 面向每一条记录计算它的分区号
    //如果有行为，不需要区分记录，本地IO拉取数据，那么这种直接IO一定比先Partition计算，然后shuffle到文件，再IO合并数据要快。
    val cartesian = rdd1.cartesian(rdd2)
    cartesian.foreach(println)

    val intersection = rdd1.intersection(rdd2)
    intersection.foreach(println)

    val subtractRDD = rdd1.subtract(rdd2)
    subtractRDD.foreach(println)

    //一个重要的API
    val kv1 = sc.parallelize(List(
      ("zhangsan", 11),
      ("zhangsan", 12),
      ("lisi", 13),
      ("wangwu", 14),
      ("zhaoliu", 15),
    ))

    val kv2 = sc.parallelize(List(
      ("zhangsan", 21),
      ("zhangsan", 22),
      ("lisi", 9),
      ("wangwu", 18),
      ("zhaoliu", 32),
    ))

    val cogroupRDD = kv1.cogroup(kv2)

    cogroupRDD.foreach(println)

    val leftJoinRDD = kv1.leftOuterJoin(kv2)
    leftJoinRDD.foreach(println)



    Thread.sleep(Long.MaxValue)
  }


}
