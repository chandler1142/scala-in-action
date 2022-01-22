package com.chandler.scala

//偏函数的定义 trait PartialFunction[-A, +B] extends (A => B)
//可以看出是一个将A类型 转换为B类型的 函数
//接受A类型为输入参数，B类型为返回值
//“-”符号作用于类型表示逆变，-A表明输入参数为A类型或A类型的父类，也就是说输入的参数应为A的子集，具有“部分”的含义。
//值域B, +表明可以是B或B的子类，具有“全部”的含义。
object Lesson07_PartialFunction {
  def main(args: Array[String]): Unit = {

    def xxx: PartialFunction[Any, String] = {
      case "hello" => "val is hello"
      case x: Int => s"$x is int"
      case _ => "none"
    }

    println(xxx(44))
    println(xxx("hello"))
    println(xxx("hi"))
  }
}
