package com.spark.training.day1

import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.sql.SparkSession

object ListProcessing1 extends App{

  val spark = SparkSession.builder().appName("ListProcessing1").master("local[1]").getOrCreate()

  import spark.implicits._

    val rdd1 = spark.sparkContext.parallelize(List(1,2,3,4,5))

        rdd1.take(2).foreach(println)

    val rdd2 = spark.read.textFile("resources/SimpleInput")

        rdd2.take(3).foreach(println)

    val rdd3 = rdd2.map(x => (x,x.length))

        rdd3.take(3).foreach(println)


}
