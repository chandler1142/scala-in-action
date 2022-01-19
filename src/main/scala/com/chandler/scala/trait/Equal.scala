package com.chandler.scala.`trait`

trait Equal {
  def isEqual(x:Any): Boolean
  def isNotEqual(x: Any): Boolean = !isEqual(x)
}
