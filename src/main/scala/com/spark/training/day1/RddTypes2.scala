package com.spark.training.day1

import org.apache.spark.sql.SparkSession

object RddTypes2 extends App {

  val spark = SparkSession.builder().appName("RddTypes2").master("local[1]").getOrCreate()

  import spark.implicits._

  val rdd = spark.sparkContext.parallelize(List(1,2,3,4,5))

    println(rdd.toDebugString)

  val rdd2 = rdd.map(x => x*2)

    println(rdd2.toDebugString)

  val rdd3 = rdd2.repartition(2)

    println(rdd3.toDebugString)

  val rdd4 = rdd3.coalesce(1)

    println(rdd4.toDebugString)

  val rdd5 = rdd4.subtract(rdd)

    println(rdd5.toDebugString)

}

