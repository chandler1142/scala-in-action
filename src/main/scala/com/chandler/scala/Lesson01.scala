package com.chandler.scala

/**
 * 流控
 */
object Lesson01 {

  def main(args: Array[String]): Unit = {
    var a = 3
    if (a < 0) {
      println(s"$a<0")
    } else {
      println(s"$a>=0")
    }

    var aa = 0
    while (aa < 10) {
      //      println(aa)
      aa += 1
    }

    println("-" * 50)

    //for循环
    val seqs: Range.Inclusive = 1 to 10
    for (i <- seqs) {
      //      println(i)
    }

    //后面的if叫做循环的守卫
    for (i <- 1 to 10 if (i % 3 == 0)) {
      println(i)
    }

    //乘法口诀表, java 编程思维
    //    for(i <- 1 to 9) {
    //      for(j <- 1 to i) {
    //        print(s"$i*$j=${i*j} ")
    //      }
    //      println()
    //    }
    for (i <- 1 to 9; j <- 1 to i) {
      print(s"$i*$j=${i * j} ")
      if (j == i) println()
    }

    //回收循环，收集变成一个集合
    val seq = for (i <- 1 to 10) yield i
    println(seq)

//    for(i <- seq) println(i)
  }
}
