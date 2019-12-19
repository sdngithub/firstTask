package com.cjc.scala.sparkpractice

import org.apache.spark.sql.SparkSession

object RddTasks {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Sum").master("local").getOrCreate()
    val sc = spark.sparkContext

    // print sum of the numbers present in the list

    /*val rdd = sc.parallelize(List(2,4,5,6,7))
    var resultRdd = rdd.reduce(_+_)
    println("Sum of numbers is : "+resultRdd)*/
    
    
    val input = "C:/Users/RSN/Desktop/emp.txt"
    
    
    val rdd1 = sc.textFile(input)
    
    
    
    val rdd2 = rdd1.map(row => row.split(",")).map(row => (row(0),row(1),row(2)))
    
    val header = rdd2.first()
    
    val skipheader = rdd2.filter(row => row != header)
    
    val rdd3 = skipheader.map(row => (row._1,row._2,(row._3.toInt),((row._3.toInt*5)/100),(row._3.toInt)+((row._3.toInt*5)/100)))
    
    rdd3.collect().foreach(println)
    
  }
}