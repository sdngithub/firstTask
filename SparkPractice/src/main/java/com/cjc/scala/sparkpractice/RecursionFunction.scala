package com.cjc.scala.sparkpractice

object RecursionFunction {
  
  def main(args: Array[String]): Unit = {
    
    var result = rcrfunction(15,2)
    println(result)
  }
  
  def rcrfunction (a:Int,b:Int) : Int = {
    var c = 0
    if (b==0)
      0
    
    else
       c = a+rcrfunction(a,b-1)
      println("value = "+c)
      c
  }
  
  
}