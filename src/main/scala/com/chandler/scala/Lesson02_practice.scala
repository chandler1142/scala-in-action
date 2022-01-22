package com.chandler.scala

object Lesson02_practice {

  def main(args: Array[String]): Unit = {
    //Scala集合类（collections）的高阶函数map，会把函数作为参数，作用与集合类的元素上
    val salaries = Seq(20000, 70000, 40000)
    //    val newSalaries = salaries.map((x: Int) => x * 2)
    val newSalaries = salaries.map(_ * 2)
    println(newSalaries)

  }


}
