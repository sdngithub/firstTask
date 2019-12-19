package com.cjc.scala.sparkpractice

import org.apache.spark.sql.SparkSession
import java.lang.System

object Aggregate {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().appName("aggr").master("local").getOrCreate()
    val sc = spark.sparkContext
    
    val rdd = sc.parallelize(List(2,4,6,8,10))
    
    
    val rddSum = rdd.reduce(_+_).toInt
    
    val rddCount = rdd.count()
    
    val avg = rddSum/rddCount
    println(avg)
  }
}