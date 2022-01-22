package com.chandler.scala

class Dog(name: String, age: Int) {

}

case class CaseDog(name: String, age: Int) {

}

object Lesson05_case_class {

  def main(args: Array[String]): Unit = {
    val dog1 = new CaseDog("Hsq", 1)
    val dog2 = new CaseDog("Hsq", 1)
    //Class都是false
    //CaseClass都是true
    println(dog1.equals(dog2))
    println(dog1 == dog2)
  }
}
