package com.chandler.spark.kafka

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  // 1597039092628
  val HOUR_FORMAT = FastDateFormat.getInstance("yyyyMMddHH")
  val DAY_FORMAT = FastDateFormat.getInstance("yyyyMMdd")


  def parseToHour(time: String) = {
    HOUR_FORMAT.format(time.toLong)
  }

  def parseToDay(time: String) = {
    DAY_FORMAT.format(time.toLong)
  }

  def main(args: Array[String]): Unit = {
    println(parseToHour("1597039092628"))
    println(parseToDay("1597039092628"))
  }

}
