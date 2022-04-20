package com.chandler.spark.mlib

import org.apache.spark.ml.linalg.DenseVector


object DenseVectorTest {
  def main(args: Array[String]): Unit = {
    val array1 = new DenseVector(Array(1,2,3))
    println( array1.toDense.toArray)
    val arrayObj = array1.toDense.toArray

    val array2 = arrayObj.+:(333)
    println(array2.getClass)
  }
}
