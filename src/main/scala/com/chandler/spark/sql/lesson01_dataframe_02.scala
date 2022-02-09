package com.chandler.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object lesson01_dataframe_02 {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("app")
      .master("local[2]")
      //      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    val rdd: RDD[String] = sc.textFile("./data/person.txt")

    //数据+元数据 = df
    val userSchema: Array[String] = Array(
      "name string",
      "age int"
    )

    //1. 数据： RDD[Row]

    def toDataType(vv: (String, Int)) = {
      userSchema(vv._2).split(" ")(1) match {
        case "string" => vv._1.toString
        case "int" => vv._1.toInt
      }
    }

    val rowRdd: RDD[Row] = rdd.map(_.split(" "))
      .map(x => x.zipWithIndex)
      // x == [(zhanagsan,0),(18,1)]
      .map(x => x.map(toDataType(_)))
      .map(x => {
        Row.fromSeq(x) //Row 代表很多列，每个列要标识出准确的类型
      })

    //2. struct type
    def getDataType(v: String) = {

      v match {
        case "string" => DataTypes.StringType
        case "int" => DataTypes.IntegerType
      }
    }

    val structFields: Array[StructField] = userSchema.map(_.split(" "))
      .map(x =>
        StructField.apply(x(0), getDataType(x(1)))
      )
    val schema = StructType(structFields)

    val schema01: StructType = StructType.fromDDL("name string,age int")

    val df: DataFrame = spark.createDataFrame(rowRdd, schema01)
    df.show()
    df.printSchema()
  }
}
