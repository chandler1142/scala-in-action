package com.chandler.spark.mlib

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.{DataFrame, SparkSession}

object lesson01_rformula {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .getOrCreate()

    val df: DataFrame = spark.read.json("data/ml/simple-ml.json").limit(4)

    println(df.toDF().show(10))


    val supervised = new RFormula()
      .setFormula("lab ~ . + color:value1 + color:value2")

    val fittedRF = supervised.fit(df)
    val preparedDF = fittedRF.transform(df)
    println(preparedDF.toDF())
    preparedDF.show(20, false)

    spark.close();
  }
}
