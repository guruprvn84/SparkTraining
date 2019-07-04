package com.spark.training.day1

import org.apache.spark.sql.SparkSession

object HelloWorld extends App {

  val spark = SparkSession.builder().appName("HelloWorld").master("local[1]").getOrCreate()

  val rdd = spark.sparkContext.parallelize(List("Hello"," World!"," Its"," Spark"))

  //rdd.foreach(print)

  println(rdd)

}
