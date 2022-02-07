package com.chandler.spark.sss.api

import java.sql.Timestamp

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.{DataFrame, SparkSession}

object lesson22_mapGroupsWithState {
  def main(args: Array[String]): Unit = {
    val path = "D:\\study\\spark\\Spark-The-Definitive-Guide\\data\\test\\"
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val dataSet = spark.read.json(path)

    val streaming: DataFrame = spark
      .readStream
      .schema(dataSet.schema)
      .option("maxFilesPerTrigger", 2)
      .json(path)

    /**
     *
     * root
     * |-- Arrival_Time: long (nullable = true)
     * |-- Creation_Time: long (nullable = true)
     * |-- Device: string (nullable = true)
     * |-- Index: long (nullable = true)
     * |-- Model: string (nullable = true)
     * |-- User: string (nullable = true)
     * |-- gt: string (nullable = true)
     * |-- x: double (nullable = true)
     * |-- y: double (nullable = true)
     * |-- z: double (nullable = true)
     */
    streaming.printSchema()

    import spark.implicits._

    val withEventTime = streaming.selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

    withEventTime
      .selectExpr("User as user", "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
      .where("activity != 'bike' and activity != 'null'")
      .as[InputRow]
      .groupByKey(_.user)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAcrossEvents)
      .writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }

  case class InputRow(user: String, timestamp: java.sql.Timestamp, activity: String)

  case class UserState(user: String, var activity: String, var start: java.sql.Timestamp, var end: java.sql.Timestamp)

  def updateUserStateWithEvent(state: UserState, input: InputRow): UserState = {
    if (Option(input.timestamp).isEmpty) {
      return state
    }
    if (state.activity == input.activity) {
      if (input.timestamp.after(state.end)) {
        state.end = input.timestamp
      }
      if (input.timestamp.before(state.start)) {
        state.start = input.timestamp
      }
    } else {
      if (input.timestamp.after(state.end)) {
        state.start = input.timestamp
        state.end = input.timestamp
        state.activity = input.activity
      }
    }
    state
  }

  def updateAcrossEvents(user: String, inputs: Iterator[InputRow], oldState: GroupState[UserState]): UserState = {
    var state: UserState = if (oldState.exists) oldState.get else UserState(user, "", new Timestamp(628416000000l), new Timestamp(6284160l))

    for (input <- inputs) {
      state = updateUserStateWithEvent(state, input)
      oldState.update(state)
    }

    state
  }

}
