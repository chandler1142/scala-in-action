package com.chandler.spark.sss.api

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object lesson22_flatMapGroupsWithState {

  val path = "D:\\study\\spark\\Spark-The-Definitive-Guide\\data\\test\\"

  case class InputRow(device: String, timestamp: java.sql.Timestamp, x: Double)

  case class DeviceState(device: String, var values: Array[Double], var count: Int)

  case class OutputRow(device: String, previousAvg: Double)

  def updateState(state: DeviceState, input: InputRow): DeviceState = {
    state.count += 1
    state.values = state.values ++ Array(input.x)
    state
  }

  //K, Iterator[V], GroupState[S]
  def updateStateAcrossEvents(device: String, inputs: Iterator[InputRow], oldState: GroupState[DeviceState]): Iterator[OutputRow] = {
    inputs.toSeq.sortBy(_.timestamp.getTime).toIterator.flatMap {
      input =>
        val state = if (oldState.exists) oldState.get else DeviceState(device, Array(), 0)
        val newState = updateState(state, input)
        if (newState.count >= 500) {
          oldState.update(DeviceState(device, Array(), 0))
          Iterator(OutputRow(device, newState.values.sum / newState.values.length.toDouble))
        } else {
          oldState.update(newState)
          Iterator()
        }
    }
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df: DataFrame = spark.read.json(path)

    val streaming: DataFrame = spark.readStream
      .schema(df.schema)
      .option("maxFilesPerTrigger", 2)
      .json(path)

    import spark.implicits._

    streaming
      .selectExpr("Device as device", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as timestamp", "x")
      .as[InputRow]
      .groupByKey(_.device)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout())(updateStateAcrossEvents)
      .writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("append")
      .start()
      .awaitTermination()
  }

}
