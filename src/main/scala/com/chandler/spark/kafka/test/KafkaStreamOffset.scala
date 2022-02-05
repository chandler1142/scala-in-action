//package com.chandler.spark.kafka.test
//
//import org.apache.kafka.common.TopicPartition
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, TaskContext}
//
//object KafkaStreamOffset {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName(this.getClass.getSimpleName)
//      .setMaster("local[2]")
//    val ssc = new StreamingContext(conf, Seconds(3))
//    ssc.sparkContext.setLogLevel("ERROR")
//
//    val groupId = "default-group"
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "localhost:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> groupId,
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )
//
//    val topics = Array("access-log-prod")
//    //todo 从存储介质上获取kafka的offset
//    val offsetsRange: collection.Map[TopicPartition, Long] = HBaseOffsetsManager.obtainOffsets(groupId, topics.head)
//
//    val stream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams, offsetsRange)
//    )
//
//    stream.foreachRDD(rdd => {
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd.foreachPartition(partitions => {
//        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//      })
//
//      rdd.foreach(record => {
//        println(record.value)
//      })
//
//      offsetRanges.map(x => HBaseOffsetsManager.storeOffset(groupId, topics.head, x))
//    })
//
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
