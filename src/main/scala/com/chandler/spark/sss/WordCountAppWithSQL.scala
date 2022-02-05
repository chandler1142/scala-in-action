package com.chandler.spark.sss

import org.apache.spark.sql.SparkSession

object WordCountAppWithSQL {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName).getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val lines = spark.readStream.format("socket")
      .option("host", "8.130.29.166")
      .option("port", 3389)
      .load()

    lines.as[String]
      .flatMap(_.split(","))
      .createOrReplaceTempView("wc")

    spark.sql(
      """
        |select
        |value, count(1) as cnt
        |from
        |wc
        |group by value
        |""".stripMargin
    ).writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()

  }
}
