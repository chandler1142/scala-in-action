package com.chandler.scala

import java.util

import scala.collection.mutable.ListBuffer

object Lesson03_Collections {

  def main(args: Array[String]): Unit = {

    //java list, 该怎么用怎么用
    val listJava = new util.LinkedList[String]()
    listJava.add("Hello")

    //scala 还有自己的collection
    //1. 数组
    //Java 中泛型<> scala中是[], 所以取数组是(n)
    //val 约等于final, 不可变描述的是val指定的引用的值(值：字面值，地址)
    val arr01 = Array[Int](1, 2, 3, 4)
    println(arr01(2))
    arr01(2) = 99
    println(arr01(2))

    //    for(elem <- arr01) {
    //      println(elem)
    //    }
    println("-----------遍历数据集------------")
    //遍历数据集，打印
    arr01.map(_ * 2).filter(_ % 4 == 0).foreach(println)


    //2. 链表
    //scala中数据集分两类：可变和不可变
    println("-----------链表-------------")
    val list01 = List(1, 2, 3, 4, 5)
    //遍历数据集，打印
    list01.map(_ * 2).filter(_ % 4 == 0).foreach(println)

    val list02 = new ListBuffer[Int]()
    list02.+=(33)
    list02.+=(34)
    list02.+=(35)
    list02.+=(36)


    println("-----------Set-------------")
    val set01 = Set(1, 2, 3, 4, 4, 3, 2, 1)

    set01.foreach(println)

    import scala.collection.mutable.Set
    val set02 = Set(11, 22, 33, 44, 55, 11)
    set02.add(88)

    set02.foreach(println)


    println("--------------Tuple-------------")
    var tuple2 = new Tuple2(1, "Chandler")
    val tuple4 = (2, "LiLei", 18, 30000)
    println(tuple2._1)
    println(tuple2._2)

    val tIter = tuple4.productIterator
    while (tIter.hasNext) {
      println(tIter.next())
    }

    println("--------------Tuple-------------")
    val map01 = Map((1, "Chandler"), (2, "Mark"))
    map01.foreach(x => println(s"${x._1}:${x._2}"))

    println(map01.get(1).get)
    println(map01.get(3))
    println(map01.get(3).getOrElse("NoName"))


    println("-------------艺术-------------------")
    //下面换成Array一样是对的
    val listStr = List(
      "hello world",
      "hello msb",
      "good idea"
    )

    //扁平化
    val flatMap01 = listStr.flatMap((x: String) => x.split(" "))
    println(flatMap01)

    //word count
    val mapList = flatMap01.map((_, 1))
    println(mapList)

    println(
      listStr.flatMap((x: String) => x.split(" "))
        .map((_, 1))
        .groupBy(_._1)
        .mapValues(_.size)
    )

    //以上代码的问题, 内存扩大了，每一步计算都留有内存对象数据
    //怎么解决？ 迭代器 iterator, 迭代器里不存数据

    println(" ------------ 艺术 升华---------------------")

    //下面的代码中，讲述了迭代器指针的不可逆性
    //如果111行不注释，113行不会打印
    val iter:Iterator[String] = listStr.iterator

    val iterFlatMap = iter.flatMap(x=> x.split(" "))
//    iterFlatMap.foreach(println)
    val iterMapList = iterFlatMap.map((_, 1))
    iterMapList.foreach(println)


    //基于迭代器的源码分析
    //1. listStr 真正的数据集，有数据的
    //2. iter.flatMap 没有真正的执行，返回了一个新的迭代器


  }
}
