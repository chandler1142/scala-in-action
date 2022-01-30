package com.chandler.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object lesson01_fromfile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkStreamingWordCount")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    println(123)
    //lines ===> DStream
    val lines: DStream[String] = ssc.textFileStream("file:///D:/tmp")
    val words = lines.flatMap(_.split(","))
    val pairs = words.map(e => (e, 1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.print()


    ssc.start()
    ssc.awaitTermination()

  }
}
