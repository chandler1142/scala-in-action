package com.chandler.delta

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object QuickStart3Streaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Streaming")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    val exampleDir = new File("/tmp/delta-streaming")
    if (exampleDir.exists()) FileUtils.deleteDirectory(exampleDir)

    println(
      "=== Section 1: write and read delta table using batch queries, and initialize table for later sections"
    )
    val data = spark.range(0, 5)
    val path = new File("/tmp/delta-streaming/delta-table").getAbsolutePath
    data.write.format("delta").save(path)

    //Read table
    val df = spark.read.format("delta").load(path)
    df.show()

    import spark.implicits._

    println("=== Section 2: write and read delta using structured streaming")
    val streamingDF = spark.readStream.format("rate").load()
    val tablePath2 = new File("/tmp/delta-streaming/delta-table2").getCanonicalPath
    val checkpointPath = new File("/tmp/delta-streaming/checkpoint").getCanonicalPath
    val stream = streamingDF
      .select($"value" as "id")
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpointPath)
      .start(tablePath2)

    stream.awaitTermination(5000)
    stream.stop()

    val stream2 = spark.readStream.format("delta")
      .load(tablePath2)
      .writeStream
      .format("console")
      .start()

    stream2.awaitTermination(10000)
    stream2.stop()


    println("=== Section 3: Streaming upserts using MERGE")

    def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long): Unit = {
      val deltaTable = DeltaTable.forPath(path)
      deltaTable
        .as("t")
        .merge(
          microBatchOutputDF.select($"value" as "id").as("s"),
          "s.id = t.id"
        )
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }

    val streamingAggDF = spark.readStream
      .format("rate")
      .load()
      .withColumn("key", col("value") % 3)
      .drop("timestamp")

    // Write the output of a streaming aggregation query into Delta Lake table
    println("Original Delta Table")
    val deltaTable = DeltaTable.forPath(path)
    deltaTable.toDF.show()

    val stream3 = streamingAggDF
      .writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("update")
      .start()

    stream3.awaitTermination(10000)
    stream3.stop()

    println("Delta Table after streaming upsert")
    deltaTable.toDF.show()


  }
}
