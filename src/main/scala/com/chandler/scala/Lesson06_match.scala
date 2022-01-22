package com.chandler.scala

object Lesson06_match {

  def main(args: Array[String]): Unit = {
    val tup = (1.0, 88, "abc", false, 'a',99)
    val iter: Iterator[Any] = tup.productIterator
    val res = iter.map(x => {
      x match {
//        case o:Int => println(s"$o 是Int类型")
        case 88 => println(s"$x ... is 88")
        case false => println(s"$x is false")
        case x:Int if x > 50 => println(s"$x > 50")
        case _ => println("我也不知道啥类型")
      }
    })

    while(res.hasNext) res.next()
  }
}
