package com.cjc.scala.sparkpractice

import org.apache.spark.sql.SparkSession



object WordCount {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().appName("Word Count").master("local").getOrCreate()
    val sc = spark.sparkContext
    
    val rdd = sc.textFile("C:/Users/RSN/Desktop/wcinput.txt")
    
   // rdd.map(x=>x.split(" ")).collect.foreach(println)
    
    val splitRdd = rdd.flatMap(x=>x.split(" "))
    
    val kvRdd = splitRdd.map(x=>(x,1))
    
    
    val reduceRdd = kvRdd.reduceByKey(_+_)
    
    reduceRdd.collect.foreach(println)
    
    
  }
}