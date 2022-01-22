package com.chandler.scala


trait God {
  def say() = {
    println("god say...")
  }
}

trait Ghost {
  def cry() = {
    println("Ghost crying...")
  }
  def scare():Unit
}

class Person(name: String) extends God with Ghost {

  def hello() = {
    println(s"$name say hello")
  }

  override def scare(): Unit = {
    println("scare...")
  }
}

object Lesson04_trait {
  def main(args: Array[String]): Unit = {
    val p1 = new Person("Chandler")
    p1.say()
    p1.cry()
    p1.hello()
    p1.scare()
  }
}
