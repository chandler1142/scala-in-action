package com.chandler.scala

class Boy(val name: String) {

}

object Lesson08_implicit_practice {

  def main(args: Array[String]): Unit = {
    //隐式转换3种情况
    //1. 调用方法的时候，方法参数声明和传入参数类型不匹配
    //即方法参数类型跟指定的不同的时候
    def printStr(name: String) = {
      println(name)
    }

    implicit def int2string(x: Int) = {
      x.toString
    }

    printStr(123)

    //2. 当对象访问一个不存在的成员的时候
    val p = new Boy("Chandler")
    implicit class plusSayName(p: Boy){
      def sayName() = {
        println(s"hello ${p.name}")
      }
    }

    p.sayName()




  }
}
