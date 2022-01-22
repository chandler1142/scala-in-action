package com.chandler.scala

import java.util

object Lesson08_Implicit {
  def main(args: Array[String]): Unit = {
    //有一些类是别人写的，你想增加一些功能

    val list = new util.LinkedList[Int]()
    list.add(1)
    list.add(2)
    list.add(3)

    //怎么给java的list 增加一个foreach的功能

    //    list.foreach(println)

    //可以这么干，但是很累
    //    val xx = new XXX(list)
    //    xx.foreach(println)

    //隐式转换: 隐式转换方法，一个输入类型，转化为一个输出类型
    // list就可以调用foreach的方法
    //利用隐式转换，让类有其他的方法
    implicit def javalistPlusForeach[T](list: util.LinkedList[T]) = {
      val iter: util.Iterator[T] = list.iterator()
      new XXX(iter)
    }

    implicit def javaArrayListPlusForeach[T](list: util.ArrayList[T]) = {
      val iter: util.Iterator[T] = list.iterator()
      new XXX(iter)
    }

    //或者用 隐式转换类
    //    implicit class XXXsdfsdf[T](list: util.LinkedList[T]) {
    //      def foreach(f: (T) => Unit): Unit = {
    //        val iter = list.iterator()
    //        while (iter.hasNext) f(iter.next())
    //      }
    //    }

    list.foreach(println)


    //隐式转换参数
    //自动寻找参数
    implicit val sdsdf: String = "lisi"
    implicit val sdsdfsdfasfs: Int = 1231

    def ooxx(implicit name: String, age: Int) = {
      println(name)
    }

//    ooxx("zhangsan")
    ooxx

    //如果有些参数需要隐式转换，有些需要手工传入

    def ooxx2(age: Int)(implicit name: String) = {
      println(s"$name:$age")
    }

    ooxx2(18)

    //尝试给一个Int类型的+函数，增加一个增加字符串的方法+Str()

    val c1 = 129

    implicit class intPlusStr(x: Int) {
      def _str(input:String) = {
        println(s"$input $x")
      }
    }

    c1._str("hello")

  }
}

class XXX[T](iter: util.Iterator[T]) {
  def foreach(f: (T) => Unit): Unit = {
    while (iter.hasNext) f(iter.next())
  }
}

//class XXX[T](list: util.LinkedList[T]) {
//  def foreach(f: (T) => Unit): Unit = {
//    val iter = list.iterator()
//    while (iter.hasNext) f(iter.next())
//  }
//}
