package com.cjc.scala.sparkpractice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameTaskOfTables {
  
  def main(args: Array[String]): Unit = {
    
    
    val spark = SparkSession.builder().appName("Ananlytics").master("local").getOrCreate()
    val sc = spark.sparkContext
    

    // Input File
    val accountInput = "C:/Users/RSN/Desktop/todaystask/df/account.txt"
    val deptInput = "C:/Users/RSN/Desktop/todaystask/df/dept.txt"
    val employeeInput = "C:/Users/RSN/Desktop/todaystask/df/employee.txt"
    val managerInput = "C:/Users/RSN/Desktop/todaystask/df/manager.txt"
    
    val inputEmpDf = spark.read.option("header","true").csv(employeeInput)
    val inputAccDf = spark.read.option("header","true").csv(accountInput)
    val inputdeptDf = spark.read.option("header","true").csv(deptInput)
    val inputMgrDf = spark.read.option("header","true").csv(managerInput)
    
    inputEmpDf.show(false)
    inputAccDf.show(false)
    inputdeptDf.show(false)
    inputMgrDf.show(false)
    
    val joinDf = inputEmpDf.join(inputAccDf,inputEmpDf.col("emp_id") === inputAccDf.col("emp_id"))
    .select(inputEmpDf.col("emp_id"), inputEmpDf.col("f_name"), inputEmpDf.col("l_name"), inputEmpDf.col("gender"), inputEmpDf.col("manager_id"), inputEmpDf.col("dept_id"), inputAccDf.col("emp_id"), inputAccDf.col("salary"))
    
    //Minimum Salary , Maximum Salary
    val maxSalDf = joinDf.select(min(col("salary")),max(col("salary"))).show()
    
    //SPARKSQL
    
    inputEmpDf.createOrReplaceTempView("emp")
    inputAccDf.createOrReplaceTempView("acc")
    inputdeptDf.createOrReplaceTempView("dept")
    inputMgrDf.createOrReplaceTempView("mgr")
    
    
    spark.sql("select f_name,salary from (select e.emp_id,e.f_name,e.l_name,e.gender,e.manager_id,e.dept_id,a.emp_id,a.salary from emp e join acc a where e.emp_id = a.emp_id) emp order by salary desc limit 1").show(false)
    
    
  }
  
}