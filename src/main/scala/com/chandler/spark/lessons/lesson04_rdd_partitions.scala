package com.chandler.spark.lessons

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object lesson04_rdd_partitions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("partitions")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data: RDD[Int] = sc.parallelize(1 to 10, 2)

    //外关联 sql 查询


    val res02: RDD[String] = data.mapPartitionsWithIndex(
      (pIndex, pIter) => {

        //在数据转换的过程中，new 集合元素，将导致内存溢出
        val lb = new ListBuffer[String]

        println("---- conn-mysql ----")
        while (pIter.hasNext) {
          val value: Int = pIter.next()
          println(s"-----$pIndex----- select $value")
          lb.+=(value + "selected")
        }
        println("-----close-mysql----------")
        lb.iterator
      }
    )

    res02.foreach(println)

    println("-----------------------------------")
    val res03: RDD[String] = data.mapPartitionsWithIndex(
      (pIndex, pIter) => {
        new Iterator[String] {
          println(s"------$pIndex---conn---mysql----")

          override def hasNext: Boolean = {
            if (pIter.hasNext == false) {
              println(s"---$pIndex---close---mysql")
              false
            } else {
              true
            }
          }

          override def next(): String = {
            val i: Int = pIter.next()
            println(s"---------$pIndex select $i----------")
            i + "selected"
          }
        }
      }
    )
    res03.foreach(println)

  }
}
