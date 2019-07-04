package com.spark.training.day1

import org.apache.spark.sql.SparkSession

object PairedRDD5 extends App{

  val spark = SparkSession.builder().appName("PairedRDD5").master("local[1]").getOrCreate()

  import spark.implicits._

  val rdd1 = spark.read.textFile("resources/SimpleInput").flatMap(x => x.split(" "))

  val pairRdd = rdd1.rdd.map(x => (x.charAt(0).toString,x))

  pairRdd.foreach(x => println(x._1 +","+x._2))

  println("---------------------1")

  val keyedRDD = rdd1.rdd.keyBy(x => x.charAt(0))

  keyedRDD.foreach(x => println(x._1+","+x._2))

  println("---------------------2")

  pairRdd.mapValues(word => word.length).foreach(x => println(x._1+","+x._2))

  println("---------------------3")

  pairRdd.keys.collect()

  println("---------------------4")

  pairRdd.values.collect()

  println("---------------------5")

  pairRdd.sortByKey().foreach(x => println(x._1 +","+ x._2))

  println("---------------------6")

  pairRdd.groupByKey.foreach(println)

  println("---------------------7")

  pairRdd.reduceByKey((a,b) => a+"-"+b).foreach(println)

  println("---------------------8")

  pairRdd.mapValues(a => a+"$$").foreach(println)

  println("---------------------9")

  pairRdd.foldByKey("")((x,y) => x+y).foreach(x => println(x._1+","+x._2))

  println("---------------------10")

  val pair1 = rdd1.rdd.map(x => (x.charAt(0).toString,x.length))
  val pair2 = rdd1.rdd.map(x => (x.charAt(0).toString,x.length*2))
  val pair3 = rdd1.rdd.map(x => (x.charAt(0).toString,x.length*4))
  val pair4 = rdd1.rdd.map(x => (x.charAt(0).toString,x.length*6))

  pair1.cogroup(pair2).take(4).foreach(println)

  pair1.cogroup(pair2,pair3,pair4).take(5).foreach(println)

  println("---------------------11")


  val examScores = Array(("Hari", 81.0), ("Hari", 94.0), ("Shiva", 91.0), ("Shiva", 93.0), ("Hari", 95.0), ("Shiva", 98.0))

  val examScoresRDD = spark.sparkContext.parallelize(examScores)

  val initialCombiner = (score:Double) => (1,score)

  val scoreCombiner = (collector:(Int,Double), score:Double) => {
        val (numScore,totalScore) = collector
    (numScore+1, totalScore+score)
  }

  val scoreMerger = (collector1:(Int,Double),collector2:(Int,Double)) => {
    val (numScore1, totalScore1) = collector1
    val (numScore2, totalScore2) = collector2
    (numScore1+numScore2,totalScore1+totalScore2)
  }

  val finalScores = examScoresRDD.combineByKey(initialCombiner,scoreCombiner,scoreMerger)

  val averagingFunction = (candidateScore: (String,(Int,Double))) => {
      val (name,(numScore,totalScore)) = candidateScore
      (name,totalScore/numScore)
  }

  val averages = finalScores.collectAsMap().map(averagingFunction)

  averages.foreach(println)

  println("---------------------12")

  /*
    ACTIONS
   */

  pairRdd.countByValue().foreach(println)

  println("---------------------13")

  pairRdd.countByKey().foreach(println)

  println("---------------------14")

  pairRdd.lookup("A").foreach(println)



}
