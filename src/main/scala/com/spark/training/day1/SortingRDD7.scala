package com.spark.training.day1


import org.apache.spark.sql.SparkSession

object SortingRDD7 extends App {

  val spark = SparkSession.builder().appName("SortingRDD7").master("local[1]").getOrCreate()

  val rdd1 = spark.sparkContext.parallelize(List(4,5,2,1)).map(x => (x,x*2))

  rdd1.foreach(println)

  rdd1.sortByKey().foreach(println)

  println("--------------------------------1")

  val rdd2 = spark.sparkContext.parallelize(List((5,1),(1,6),(3,8))).map(x => (x,x._1*3))

  rdd2.foreach(println)

  implicit val sortInteger = new Ordering[(Int,Int)]{
    override def compare(a:(Int,Int),b:(Int,Int)) = (a._1+a._2).compare(b._1+b._2)
  }


  rdd2.sortByKey(false).foreach(println)

  rdd2.sortByKey(true).foreach(println)
}


