//package com.chandler.spark.kafka.test
//
//import java.util
//
//import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result, Scan}
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
//import org.apache.kafka.common.TopicPartition
//import org.apache.spark.streaming.kafka010.OffsetRange
//
//object HBaseOffsetsManager {
//
//  private val configuration = HBaseConfiguration.create()
//  configuration.set("hbase.zookeeper.quorum", "localhost:2182")
//
//  private val connection = ConnectionFactory.createConnection(configuration)
//  private val tableName = "kafka_offsets"
//
//  def obtainOffsets(groupId: String, topic: String): collection.Map[TopicPartition, Long] = {
//    val table = connection.getTable(TableName.valueOf(tableName))
//    val scan = new Scan()
//    scan.withStartRow(Bytes.toBytes(s"$groupId,$topic"))
//    scan.withStartRow(Bytes.toBytes(s"$groupId,$topic"))
//
//    val resultMap = collection.mutable.Map[TopicPartition, Long]()
//    val iterator: util.Iterator[Result] = table.getScanner(scan).iterator()
//    while (iterator.hasNext) {
//      val result: Result = iterator.next()
//      while (result.advance()) {
//        val cell: Cell = result.current()
//        //rowkey = groupId,topic
//        val row = new String(CellUtil.cloneRow(cell))
//        //family = partitions
//        val family = new String(CellUtil.cloneFamily(cell))
//        //partitionId
//        val qualifier = Bytes.toInt(CellUtil.cloneQualifier(cell))
//        //offset value
//        val value = Bytes.toLong(CellUtil.cloneValue(cell))
//        println(s"$row | $family | $qualifier | $value")
//        resultMap.put(new TopicPartition(topic, qualifier), value)
//      }
//    }
//
//    if (null != table) {
//      table.close()
//    }
//
//    resultMap
//  }
//
//  def storeOffset(groupId: String, topic: String, offsetRange:OffsetRange) = {
//    val table = connection.getTable(TableName.valueOf(tableName))
//    val rowKey = s"$groupId,$topic"
//
//    val put = new Put(rowKey.getBytes)
//    put.addColumn(Bytes.toBytes("partitions"), Bytes.toBytes(offsetRange.partition), Bytes.toBytes(offsetRange.untilOffset))
//    table.put(put)
//
//    if (null != table) {
//      table.close()
//    }
//
//  }
//}
