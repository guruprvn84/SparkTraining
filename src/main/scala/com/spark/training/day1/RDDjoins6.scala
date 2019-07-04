package com.spark.training.day1

import org.apache.spark.sql.SparkSession

object RDDjoins6 extends App {

  val spark = SparkSession.builder().appName("RDDjoins6").master("local[1]").getOrCreate()

  val rdd1 = spark.sparkContext.parallelize(List(1,2,3,4,5)).map(x => (x,x+1))

  val rdd2 = spark.sparkContext.parallelize(List(1,3,5)).map(x => (x,x*3))

  rdd1.join(rdd2).foreach(println)

  println("----------------------1")

  rdd1.leftOuterJoin(rdd2).foreach(println)

  println("----------------------2")

  rdd1.rightOuterJoin(rdd2).foreach(println)

  println("----------------------3")

  rdd1.cartesian(rdd2).foreach(println)

  println("----------------------4")
}
