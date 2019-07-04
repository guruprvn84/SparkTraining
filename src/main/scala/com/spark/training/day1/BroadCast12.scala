package com.spark.training.day1

import org.apache.spark.sql.SparkSession

object BroadCast12 extends App {

  val spark = SparkSession.builder().appName("BroadCast").master("local[3]").getOrCreate()

  val commonWords = List("spark","scala","sql","apache")

  val commonWordsBC = spark.sparkContext.broadcast(commonWords)

  val file = spark.sparkContext.textFile("resources/ReplicatedInput.txt")

  def toWords(line:String):List[Option[String]] = {

    line.split(" ").map(word => {
        if(commonWordsBC.value contains word.toLowerCase.trim.replaceAll("[^a-z]",""))
            None
        else
          Some(word)
      }).toList
  }

  val uncommonWords = file.flatMap(toWords)

  uncommonWords.map(word => {
      word match {
        case Some(x) => println(x)
        case None =>
      }

  }).collect()

}
