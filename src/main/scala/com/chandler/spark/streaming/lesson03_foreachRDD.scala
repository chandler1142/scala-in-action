package com.chandler.spark.streaming

import java.sql.Connection

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 统计分析，并把结果写入到MySQL中
 */
object lesson03_foreachRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    ssc.checkpoint("ckpt")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("8.130.29.166", 3389)
    val result: DStream[(String, Int)] = lines.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)

    RDD
    //把结果通过foreachRDD通过算子输出到MySQL
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val connection: Connection = MySQLUtils.getConnection()

        partition.foreach(pair => {
          val sql = s"insert into wc(word,cnt) values ('${pair._1}','${pair._2}')"
          connection.createStatement().execute(sql)
        })

        MySQLUtils.close(connection)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
