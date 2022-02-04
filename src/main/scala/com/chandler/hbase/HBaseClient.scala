package com.chandler.hbase

import java.util

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}

object HBaseClient {

  private val configuration = HBaseConfiguration.create()
  configuration.set("hbase.zookeeper.quorum", "localhost:2182")

  private val connection = ConnectionFactory.createConnection(configuration)

  def main(args: Array[String]): Unit = {
//        println(getTable("user"))
//    insert("user")
    query("user")
  }

  def getTable(tableName: String) = {
    connection.getTable(TableName.valueOf(tableName))
  }

  def insert(tableName: String) = {
    val table = getTable(tableName)
    val put = new Put("3".getBytes())

    put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("name"), Bytes.toBytes("chandler"))
    put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("age"), Bytes.toBytes("21"))
    put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("sex"), Bytes.toBytes("m"))

    table.put(put)

    if (null != table) {
      table.close()
    }
  }

  def query(tableName: String) = {
    val table = getTable(tableName)
    val scan = new Scan()
    scan.withStartRow("1".getBytes)
    scan.withStopRow("4".getBytes)
    scan.addColumn("o".getBytes, "name".getBytes)
    val iterator: util.Iterator[Result] = table.getScanner(scan).iterator()

    while(iterator.hasNext) {
      val result: Result = iterator.next()
      while(result.advance()) {
        val cell: Cell = result.current()
        val row = new String(CellUtil.cloneRow(cell))
        val family = new String(CellUtil.cloneFamily(cell))
        val qualifier = new String(CellUtil.cloneQualifier(cell))
        val value= new String(CellUtil.cloneValue(cell))

        println(s"$row | $family | $qualifier | $value")
      }
    }


    if (null != table) {
      table.close()
    }

  }

}
