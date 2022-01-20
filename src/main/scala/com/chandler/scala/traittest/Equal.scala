package com.chandler.scala.traittest

trait Equal {
  def isEqual(x:Any): Boolean
  def isNotEqual(x: Any): Boolean = !isEqual(x)
}
