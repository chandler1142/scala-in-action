package com.chandler.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object lesson03_updateStateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkStreamingWordCount")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    ssc.checkpoint("ckpt")
    //lines ===> DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("8.130.29.166", 3389)


    val words = lines.flatMap(_.split(","))
    val pairs = words.map(e => (e, 1))
    //将方法转为函数
    val wordCounts = pairs.updateStateByKey[Int](updateFunction _)
    wordCounts.print()


    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    //使用新值结合已有的老的值进行func的操作
    val current: Int = newValues.sum
    val old: Int = runningCount.getOrElse(0)
    Some(current + old)
  }
}
