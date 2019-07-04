package com.spark.training.day1

import org.apache.spark.sql.SparkSession

object PartitionAndCoalesce10 extends App {

  val spark = SparkSession.builder().appName("PartitionAndCoalesce").master("local[1]").getOrCreate()

  import spark.implicits._

  val x = spark.sparkContext.parallelize((1 to 12),4)

  println(x.partitions.size)

  val df = x.toDF("numbers")

  df.foreachPartition(x => {
    x.foreach(print)
    println("----")
  })

  println("--------------------------1")

  df.repartition(2).foreachPartition(x => {
    x.foreach(print)
    println("----")
  })

  println("--------------------------2")

  df.repartition(6).foreachPartition(x => {
    x.foreach(print)
    println("----")
  })

  println("--------------------------3")

  df.coalesce(2).foreachPartition(x => {
    x.foreach(print)
    println("----")
  })

  println("--------------------------4")

  df.coalesce(4).foreachPartition(x => {
    x.foreach(print)
    println("----")
  })

  println("--------------------------5")

  df.coalesce(6).foreachPartition(x => {
    x.foreach(print)
    println("----")
  })

}
