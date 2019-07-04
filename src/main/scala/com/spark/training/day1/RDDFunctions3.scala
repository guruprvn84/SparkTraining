package com.spark.training.day1

import com.spark.training.day1.ListProcessing1.spark
import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.sql.SparkSession

object RDDFunctions3 extends App {

  val spark = SparkSession.builder().appName("RDDFunctions3").master("local[1]").getOrCreate()

  import spark.implicits._

  val rdd1 = spark.read.textFile("resources/SimpleInput")

  val list1 = rdd1.rdd.map(x => x.length).collect()

    println(list1)

  println("-------------------------------1")

  val rdd2 = rdd1.rdd.flatMap(x => x.split(" "))

    rdd2.take(2).foreach(println)

  println("-------------------------------2")

  rdd1.rdd.map(x => x.split(" ")).take(2).foreach(println)

  println("-------------------------------3")

  rdd2.filter(x => x.length > 10).foreach(println)

  println("-------------------------------4")

  val rdd3 = rdd2.groupBy(x => x.length)

  rdd3.foreach(println)

  println("-------------------------------5")

  val rdd7 = spark.sparkContext.parallelize(1 to 6 )

  val rdd8 = spark.sparkContext.parallelize(5 to 10 )

  rdd7.union(rdd8).collect().foreach(println)

  rdd7.intersection(rdd8).foreach(println)

  println("-------------------------------8")

  println(rdd7.fold(0)(_ + _))

  println("-------------------------------9")

  rdd7.zip(rdd8).foreach(println) //Imp: partition number

  println("-------------------------------10")

  val rdd9 = spark.sparkContext.parallelize(1 to 100,10)

  val result = rdd9.aggregate(0)(
    (a,b) => Math.max(a,b)
            , (x,y)=> x+y)

  println(result)

  println("-------------------------------11")








}
