package com.cjc.scala.sparkpractice

import org.apache.spark.sql.SparkSession

object RddTaskOfTables {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Ananlytics").master("local").getOrCreate()
    val sc = spark.sparkContext

    // Input File
    val accountInput = "C:/Users/RSN/Desktop/todaystask/account.txt"
    val deptInput = "C:/Users/RSN/Desktop/todaystask/dept.txt"
    val employeeInput = "C:/Users/RSN/Desktop/todaystask/employee.txt"
    val managerInput = "C:/Users/RSN/Desktop/todaystask/manager.txt"

    // Input Rdd
    val accountInputRdd = sc.textFile(accountInput)
    val deptInputRdd = sc.textFile(deptInput)
    val employeeInputRdd = sc.textFile(employeeInput)
    val managerInputRdd = sc.textFile(managerInput)

    // Split data by tab
    val fieldsAccountInputRdd = accountInputRdd.map(row => row.split("\t")).map(row => (row(0), row(1)))
    val fieldsDeptInputRdd = deptInputRdd.map(row => row.split("\t")).map(row => (row(0), row(1)))
    val fieldsEmployeeInputRdd = employeeInputRdd.map(row => row.split("\t")).map(row => (row(0), row(1), row(2), row(3), row(4), row(5)))
    val fieldsManagerInputRdd = managerInputRdd.map(row => row.split("\t")).map(row => (row(0), row(1)))

    // Skip Header of Account
    val accountHeaderRdd = fieldsAccountInputRdd.first()
    val accountHeaderSkipRdd = fieldsAccountInputRdd.filter(row => row != accountHeaderRdd)
    val accountRdd = accountHeaderSkipRdd.map(row => (row._1, row._2))
    accountRdd.collect().foreach(println)

    // Skip Header of Dept
    val deptHeaderRdd = fieldsDeptInputRdd.first()
    val deptHeaderSkipRdd = fieldsDeptInputRdd.filter(row => row != deptHeaderRdd)
    val deptRdd = deptHeaderSkipRdd.map(row => (row._1, row._2))
    deptRdd.collect().foreach(println)

    // Skip Header of Employee
    val employeeHeaderRdd = fieldsEmployeeInputRdd.first()
    val employeeHeaderSkipRdd = fieldsEmployeeInputRdd.filter(row => row != employeeHeaderRdd)
    val employeeRdd = employeeHeaderSkipRdd.map(row => (row._1, row._2, row._3, row._4, row._5, row._6))
    employeeRdd.collect().foreach(println)

    // Skip Header of MAnager
    val managerHeaderRdd = fieldsManagerInputRdd.first()
    val managerHeaderSkipRdd = fieldsManagerInputRdd.filter(row => row != managerHeaderRdd)
    val managerRdd = managerHeaderSkipRdd.map(row => (row._1, row._2))
    managerRdd.collect().foreach(println)

    // Find Highest Salary of employee show employee name and salary
    val employeeKeyValRdd = employeeRdd.map(row => (row._1, (row._2, row._3, row._4, row._5, row._6)))
    employeeKeyValRdd.collect().foreach(println)
    val salaryRdd = employeeKeyValRdd.join(accountRdd)
    salaryRdd.collect().foreach(println)
    val sortedSalaryRdd = salaryRdd.map(row => (row._2._1._1, row._2._1._2, row._2._2))
    val maxSalRdd = sortedSalaryRdd.max
    println(maxSalRdd)

    // Find dept wise highest salary
    val tempSalaryRdd = salaryRdd.map(row => (row._2._1._5, (row._1, row._2)))
    val deptSalaryRdd = deptRdd.join(tempSalaryRdd)
    deptSalaryRdd.collect().foreach(println)
    val highestDeptRdd = deptSalaryRdd.map(row => (row._2._2._2._1._1, row._2._2._2._1._2, row._2._1, row._2._2._2._2))
    highestDeptRdd.collect().foreach(println)
    highestDeptRdd.map(row => (row._3, (row._1, row._2, row._4))).groupByKey().mapValues(row => row.max).collect().foreach(println)

    // Department wise female
    employeeRdd.collect().foreach(println)
    val empRdd = employeeRdd.map(row => (row._6, (row._1, row._2, row._3, row._4, row._5)))
    empRdd.collect().foreach(println)
    val deptEmpJoinRdd = deptRdd.join(empRdd)
    deptEmpJoinRdd.filter(row => row._2._2._4.contains("female")).collect().foreach(println)
    deptEmpJoinRdd.filter(row => row._2._2._4.contains("female")).groupByKey.mapValues(r => r.size).collect.foreach(println)

    // Department wise male
    deptEmpJoinRdd.filter(row => row._2._2._4.contentEquals("male")).collect().foreach(println)
    deptEmpJoinRdd.filter(row => row._2._2._4.contentEquals("male")).groupByKey.mapValues(r => r.size).collect.foreach(println)

  }

}