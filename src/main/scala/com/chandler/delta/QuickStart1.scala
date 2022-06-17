package com.chandler.delta

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

import java.io.File

object QuickStart1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Quickstart")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").enableHiveSupport()
      .getOrCreate()

    val file = new File("/tmp/delta-table")
    if (file.exists()) {
      FileUtils.deleteDirectory(file)
    }

    // Create a table
    println("Creating a table")
    val path = file.getCanonicalPath
    var data = spark.range(0, 5)
    data.write.format("delta").save(path)

    // Read table
    println("Reading the table")
    val df = spark.read.format("delta").load(path)
    df.show()

    // Upsert (merge) new data
    println("Upsert new data")
    val newData = spark.range(0, 20).toDF()
    val deltaTable = DeltaTable.forPath(path)

    deltaTable
      .as("oldData")
      .merge(newData.as("newData"), "oldData.id = newData.id")
      .whenMatched()
      .update(Map("id" -> col("newData.id")))
      .whenNotMatched()
      .insert(Map("id" -> col("newData.id")))
      .execute()


    // Delete every even value
    deltaTable.delete(condition = expr("id % 2 == 0"))

    spark.read.format("delta").load(path).toDF().show()


    FileUtils.deleteDirectory(file)
    spark.close()
  }
}
