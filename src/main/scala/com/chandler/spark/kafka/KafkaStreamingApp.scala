package com.chandler.spark.kafka

import com.chandler.hbase.HBaseClient
import org.apache.hadoop.hbase.client.Table
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreamingApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[2]")
    //    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val groupId = "default-group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      //      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("access-log-prod")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val logStream = stream.map(x => {
      val splits = x.value().split("\t")
      //抓取开始时间，时长，用户名 ===> 时间+用户 时长 ===> wc的变种
      (DateUtils.parseToHour(splits(0).trim), splits(1).toLong, splits(5).trim)
    })


    /**
     * 统计结果 ===> 存储DB
     *
     * 每个用户每个小时使用时长
     * 用户，每个小时，时长
     */
    //    logStream.map(x => {
    //      ((x._1, x._3), x._2)
    //    }).reduceByKey(_ + _)
    //      .foreachRDD(rdd => {
    //        rdd.foreachPartition(partition => {
    //          val table: Table = HBaseClient.getTable("access_user_hour")
    //          partition.foreach(x => {
    //            val rowKey = s"${x._1._1}_${x._1._2}"
    //            table.incrementColumnValue(
    //              rowKey.getBytes,
    //              "o".getBytes,
    //              "time".getBytes,
    //              x._2
    //            )
    //          })
    //
    //          if (table != null) {
    //            table.close()
    //          }
    //        })
    //      })

    /**
     * 统计每个用户每天的使用时长
     */
    logStream.map(x => {
      ((x._1.substring(0, 8), x._3), x._2)
    }).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val table: Table = HBaseClient.getTable("access_user_day")
          partition.foreach(x => {
            val rowKey = s"${x._1._1}_${x._1._2}"
            table.incrementColumnValue(
              rowKey.getBytes,
              "o".getBytes,
              "time".getBytes,
              x._2
            )
          })

          if (table != null) {
            table.close()
          }
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
