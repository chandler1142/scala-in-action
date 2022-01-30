package com.chandler.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object lesson02_transform {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkStreamingWordCount")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.sparkContext.setLogLevel("ERROR")

    val data = List("chan")
    val dataRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(data).map(x => (x, false))

    //lines ===> DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("8.130.29.166", 3389)

    /**
     * 20221212, chan
     * 20221213, test
     */
    lines.map(x => (x.split(",")(1), x))
      .transform(rdd => {
        rdd.leftOuterJoin(dataRDD)
          .filter(x => {
            x._2._2.getOrElse(false) != true
          })
      }).print()


    ssc.start()
    ssc.awaitTermination()
  }
}
