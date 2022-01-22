package com.chandler.scala

import java.util.Date

/**
 * 方法
 */
object Lesson02 {

  def out_func01 = {
    println("I am out func01")
  }

  def main(args: Array[String]): Unit = {

    println("-" * 50)

    //无返回
    def func01(): Unit = {
      println("func01")
    }

    func01()

    var x = 3
    var y = func01()
    println(y)


    //有返回值
    def func02(): Int = {
      3
    }

    println(func02())

    //参数
    def func03(a: String) = {
      println(a)
    }

    func03("hello world")

    //递归函数
    def func04(num: Int): Int = {
      if (num == 0) return 1;
      if (num == 1) return 1;
      return func04(num - 1) + func04(num - 2)
    }


    //默认值函数
    def func05(a: Int = 8, b: String = "abc"): Unit = {
      println(s"$a\t$b")
    }

    func05()
    func05(123)
    func05(b = "ooxx")

    //匿名函数
    println("--------- 匿名函数 ----------")
    //函数是第一类值
    //注意函数的类型
    var func06: (Int, Int) => Int = (a: Int, b: Int) => {
      a + b
    }

    var value = func06(3, 5)
    println(value)

    println("--------------嵌套函数--------------")

    def func07(a: String) = {

      //作用域
      //子作用域里面是可以看见父作用域的内容的
      def func05() = {
        println(a)
      }

      func05()
    }

    func07("hello")

    println("-------------偏应用函数-------------")

    def func08(date: Date, tp: String, msg: String) = {
      println(s"$date\t$tp\t$msg")
    }

    func08(new Date(), "info", "ok")

    var info = func08(_: Date, "info", _: String)
    info(new Date(), "123123")

    println("------------ 可变参数 -------------")

    def afunc: Int => Unit = (a: Int) => {
      print(s"$a\t")
    }

    def func09(a: Int*) = {
      //      a.foreach(x => print(s"$x\t"))

      //      a.foreach(print)
      a.foreach(afunc)
      println()
    }

    func09(3)
    func09(3, 4, 5, 6, 7)


    println("------------ 高阶函数 -------------")
    //函数作为参数，函数作为返回值

    //函数作为参数
    def computer(a: Int, b: Int, f: (Int, Int) => Int): Unit = {
      println(f(a, b))
    }

    computer(3, 8, (x: Int, y: Int) => {
      x + y
    })
    computer(3, 8, _ + _)

    //函数作为返回值
    def factory(i: String): (Int, Int) => Int = {
      def plus(x: Int, y: Int): Int = {
        x + y
      }

      if (i.equals("+")) {
        plus
      } else {
        (x: Int, y: Int) => {
          x * y
        }
      }
    }

    computer(3, 8, factory("+"))
    computer(3, 8, factory("*"))


    def factory2(i: String): (Int, Int) => Int = i match {
      case "+" => _ + _
      case "*" => _ * _
    }

    computer(3, 8, factory2("+"))
    computer(3, 8, factory2("*"))

    println("---------------柯里化------------------")

    def func11(a: Int)(b: Int)(c: String) = {
      println(s"$a\t$b\t$c")
    }

    func11(1)(2)("abc")

    def func12(a: Int*)(b: String*) = {
      a.foreach(print)
      println()
      b.foreach(print)
      println()
    }

    func12(1, 2, 3)("a", "b")

    println("-----------*.方法-------------")
    //方法不想执行，赋值给一个引用，方法名+空格+下划线
    val func13 = out_func01 _
    //真正执行
    func13()

    //scala一切皆是对象

  }
}
