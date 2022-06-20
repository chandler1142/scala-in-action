package com.chandler.performance


import au.com.bytecode.opencsv.CSVWriter

import java.io.{BufferedWriter, FileWriter}
import java.util

object MockApp01 {
  def main(args: Array[String]): Unit = {
    val csvFields = Array("id", "score", "sex", "name")

    val nameValues = List("Chandler", "Tom", "Fiona", "Jerry")
    val sexValues = List(0, 1)

    val outputFile = new BufferedWriter(new FileWriter("DataSkewing.csv"))
    val csvWriter = new CSVWriter(outputFile, ',', '\u0000')

    val listOfRecords: util.List[Array[String]] = new util.ArrayList[Array[String]]

    listOfRecords.add(csvFields)

    Math.random()
    //如果序号大于80万，就选择800008，这样对10取模，数据就会倾斜到第8个任务上
    for (i <- 1 to 10000000) {
      if (i < 8000000) {
        listOfRecords.add(Array(i.toString, (Math.random() * 100).toString, "1", nameValues((Math.random() * 4).toInt)))
      } else {
        listOfRecords.add(Array(8000008.toString, (Math.random() * 100).toString, "1", nameValues((Math.random() * 4).toInt)))
      }
    }


    csvWriter.writeAll(listOfRecords)

    csvWriter.close()
    outputFile.close()

  }
}
