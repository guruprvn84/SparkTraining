package com.spark.training.day1

import org.apache.spark.{HashPartitioner, RangePartitioner}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object DataPartitioning9 extends App {

  val spark = SparkSession.builder().appName("DataPartitioning9").master("local[1]").getOrCreate()

  val pairRdd = spark.sparkContext.parallelize(generateTuples())

  pairRdd.take(10).foreach(println)

  println(pairRdd.partitioner)

  println(pairRdd.getNumPartitions)

  println("-----------------------Hash")
  hashPartition()

  println("-----------------------Range")
  rangePartition()

  def generateTuples():List[(String,Int)]={
    val alpha = "abcdefghijklmnopqrstuvwxyz"
    val r = scala.util.Random
    (0 to 100).map(x => {
      val next = r.nextInt(24)
      (alpha.substring(next,next+3),x)
    }).toList
  }

  def hashPartition()={

    val partitionedRdd = pairRdd.partitionBy(new HashPartitioner(5))

    println(partitionedRdd.partitioner)

    println(partitionedRdd.getNumPartitions)

    partitionedRdd.mapPartitions(x => x.take(2)).foreach(print)

  }

  def rangePartition()={
    val rangePartitioner = new RangePartitioner(8,pairRdd)

    val rangePartitioned = pairRdd.partitionBy(rangePartitioner)

    println(rangePartitioned.partitioner)

    println(rangePartitioned.getNumPartitions)

    rangePartitioned.mapPartitions(x => x.take(2)).foreach(print)

  }

}
