package com.chandler.scala.traittest

object Test {
  def main(args: Array[String]): Unit = {
    val p1 = new Point(2, 3)
    var p2 = new Point(2, 4)
    var p3 = new Point(3, 3)

    println(p1.isEqual(p2))
    println(p1.isNotEqual(p3))
    println(p1.isNotEqual(2))
  }
}
