//package com.chandler.spark.kafka.test
//
//import com.chandler.spark.kafka.DateUtils
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object KafkaStreamingCount {
//  def main(args: Array[String]): Unit = {
//    val sparkConf: SparkConf = new SparkConf()
//      .setAppName(this.getClass.getSimpleName)
//      .setMaster("local[2]")
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//
//    val ssc = new StreamingContext(sparkConf, Seconds(3))
//
//    val groupId = "default-group"
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "localhost:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> groupId,
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (true: java.lang.Boolean)
//    )
//    ssc.checkpoint("ckpt")
//
//    val topics = Array("access-log-prod")
//    val stream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams)
//    )
//
//
//    //需求1: 统计PV，即单日页面访问数
//    //    val dataRDD: DStream[((String, String), Int)] = stream.map(x => {
//    //      val splits: Array[String] = x.value().split("\t")
//    //      //日期，页面代号, 计数1
//    //      ((DateUtils.parseToDay(splits(0)), splits(3)), 1)
//    //    })
//    //
//    //    val reducedRDD: DStream[((String, String), Int)] = dataRDD.reduceByKey(_ + _)
//    //    val accumulatedDataRDD: DStream[((String, String), Int)] = reducedRDD.updateStateByKey(updateFunction _)
//    //
//    //    accumulatedDataRDD.print()
//    //
//
//    //需求2: 统计UV，即单日用户访问数
//    val dataRDD = stream.map(x => {
//      val splits: Array[String] = x.value().split("\t")
//      //日期, 用户名称, 计数1
//      ((DateUtils.parseToDay(splits(0)), splits(5)), 1)
//    })
//
//    val reduceRDD: DStream[((String, String), Int)] = dataRDD.reduceByKey(_ + _)
//    val accumulatedDataRDD: DStream[((String, String), Int)] = reduceRDD.updateStateByKey(updateFunction _)
//
//    accumulatedDataRDD.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
//    //使用新值结合已有的老的值进行func的操作
//    val current: Int = newValues.sum
//    val old: Int = runningCount.getOrElse(0)
//    Some(current + old)
//  }
//
//}
