package com.chandler.scala.clazztest

class SameFieldTest {
  var name = "Chandler"

  def printText: Unit = {
    println(name)
  }
}

object SameFieldTest {
  var name = "ChandlerZhao"

  def main(args: Array[String]): Unit = {
    val obj = new SameFieldTest()
    println(SameFieldTest.name)
    println(obj.name)
  }
}

//object Main {
//  def main (args: Array[String] ): Unit = {
//    val sameFieldTest = new SameFieldTest()
//    println(sameFieldTest)
//    println(SameFieldTest.name1)
//  }
//}
