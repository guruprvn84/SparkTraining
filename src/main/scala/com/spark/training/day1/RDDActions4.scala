package com.spark.training.day1


import org.apache.spark.sql.SparkSession

object RDDActions4 extends App {

  val spark = SparkSession.builder().appName("RDDActions4").master("local[1]").getOrCreate()

  val rdd = spark.sparkContext.parallelize(1 to 100,4)

  println(rdd.reduce(_+_))

  println(rdd.collect())

  println(rdd.count())

  println(rdd.take(2))

  println(rdd.takeOrdered(10))

  spark.sparkContext.parallelize(List(1,2,3,4,5,1,2,3,4,5,1,5)).countByValue().foreach(println)

  val rdd1 = spark.read.textFile("resources/SimpleInput").rdd.flatMap(x => x.split(" "))

  rdd1.reduce((x,y) =>
    if(x.length > y.length)
      x.substring(y.length)
    else
      y.substring(x.length)
  ).foreach(println)

}
