package com.chandler.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object lesson01_wordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkStreamingWordCount")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    //lines ===> DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("8.130.29.166", 3389)

    val words = lines.flatMap(_.split(","))
    val pairs = words.map(e => (e, 1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.print()


    ssc.start()
    ssc.awaitTermination()

  }
}
