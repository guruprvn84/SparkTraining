package com.spark.training.day1

import org.apache
import org.apache.spark
import org.apache.spark.sql.SparkSession


object RDDPartitionFunctions3 extends App{


  val spark = SparkSession.builder().appName("RDDPartitionFunctions4").master("local[1]").getOrCreate()

  val rdd5 = spark.sparkContext.parallelize(1 to 9, 3)

  println(rdd5.partitions)

  rdd5.mapPartitions(x => List(x.next).iterator).collect.foreach(println)

  println("-------------------------------0")

  rdd5.mapPartitions(x => x).foreach(x => println(x))

  println("-------------------------------1")

  val rdd6 = spark.sparkContext.parallelize(1 to 9, 3)

  rdd6.mapPartitionsWithIndex((index:Int, it:Iterator[Int]) => it.toList.map(x => (index,x)).iterator).collect.foreach(println)

  println("-------------------------------2")

  rdd6.glom().collect().foreach(x => {x.foreach(print); println()})

  println("-------------------------------3")

  rdd6.foreachPartition(it => println(it.toList))



}
