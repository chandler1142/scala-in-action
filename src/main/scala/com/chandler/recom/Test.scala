package com.chandler.recom

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object Test {

  case class UserItem(userId: String, itemId: String, score: Double)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", true)
      .option("delimiter", ",")
      .option("inferSchema", true)
      .schema(ScalaReflection.schemaFor[UserItem]
        .dataType.asInstanceOf[StructType])
      .load("data/recom/cf_user_based.csv")

    df.show(false)
    df.printSchema()

    //通过预先相似度计算用户的相似度
    //分母 每个向量的模的乘积

    import spark.implicits._
    val dfScore: DataFrame = df.rdd.map(x => (x(0).toString, x(2).toString))
      .groupByKey()
      .mapValues(score => math.sqrt(
        score.toArray.map(
          itemScore => math.pow(itemScore.toDouble, 2)
        ).reduce(_ + _)
      ))
      .toDF("userId", "mod")

    dfScore.show(false)
  }
}
