package com.chandler.scala.traittest

class Point(xc: Int, yc: Int) extends Equal {
  var x: Int = xc
  var y: Int = yc

  override def isEqual(obj: Any): Boolean =
    obj.isInstanceOf[Point] && obj.asInstanceOf[Point].x == x
}