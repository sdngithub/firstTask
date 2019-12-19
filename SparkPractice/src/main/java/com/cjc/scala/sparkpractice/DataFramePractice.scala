package com.cjc.scala.sparkpractice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DataFramePractice {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().appName("Practice").master("local").getOrCreate()    
    val sc = spark.sparkContext    
    val sqlc = spark.sqlContext
    
    val inputPath = "C:/Users/RSN/Desktop/MOCK_DATA.csv"    
    val inputRdd = sc.textFile(inputPath)
    
    //RDD WITH HEADER SKIP
    
    /*val fieldFilterRdd = inputRdd
    .map(row => row.split(","))
    .map(row=>(row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7)))    
    val header = fieldFilterRdd.first()
    val headerSkip = fieldFilterRdd.filter(line => line != header)
    val fieldsRdd = headerSkip.map(row=>(row._1,row._2,row._3,row._4,row._5,row._6,row._7.toFloat,row._8))
    */
    //COUNT MALE & FEMALE
    
    /*val keyValRdd = fieldsRdd.map(row=>(row._5,1))    
    val cntRdd = keyValRdd.reduceByKey(_+_)    
    cntRdd.collect().foreach(println)*/
    
    //SUM OF SALARY BY GENDER
    
   /* val keyValRdd = fieldsRdd.map(row=>(row._5,row._7.toDouble))    
    val sumRdd = keyValRdd.reduceByKey(_+_)    
    sumRdd.collect().foreach(println)*/
    
    // MAX SALARY BY GENDER
    
    /*val keyValRdd = fieldsRdd.map(row=>(row._5,row._7.toFloat))
    keyValRdd.groupByKey().mapValues(row=>row.max)    
    .collect().foreach(println)
    */
    
    // HIGHEST SALARY DEPT WISE
    
    /*val keyValRdd = fieldsRdd.map(row=>(row._8,row._7.toFloat))    
    keyValRdd.groupByKey().mapValues(row=>row.max)    
    .collect().foreach(println)
    */
    
    // SECOND HIGHEST SALARY DEPT WISE (BY USING SPARK SQL)
       
    import spark.implicits._
    
    //DATAFRAME (DIRECT FILE READ)
    
    val inputDf = spark.read.option("header", "true").csv(inputPath)
    val fieldsDf = inputDf.select(col("id"),col("first_name"),col("last_name"),col("email"),col("gender"),col("joining_date"),col("salary"),col("dept"))
    
    val windowSpec = Window.partitionBy("dept").orderBy(col("salary").desc)
    fieldsDf.withColumn("row_number", row_number().over(windowSpec))
    .filter(col("row_number")===2)
    .show(false)
    
    
    //SPARK SQL
    
    /*fieldsDf.createOrReplaceTempView("emp")
    spark.sql("select *, row_number() over(order by salary desc) as rw_num from emp").show(false)*/
    
    
    //DATAFRAME (RDD TO DATAFRAME WITH HEADER SKIP)
    
    /*val fieldFilterRdd = inputRdd
    .map(row => row.split(","))
    .map(row=>(row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7)))
    
    val header = fieldFilterRdd.first()
    val headerSkip = fieldFilterRdd.filter(line => line != header)
    
    val fieldsDf = headerSkip.toDF("id","f_name","l_name","email","gender","joining_date","salary","dept")
    
     fieldsDf.show(false)*/
    
       

  }

}