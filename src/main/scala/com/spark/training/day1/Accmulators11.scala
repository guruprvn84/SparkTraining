package com.spark.training.day1

import org.apache.spark.sql.SparkSession

object Accmulators11 extends App {

  val spark = SparkSession.builder().appName("Accmulators").master("local[3]").getOrCreate()

  val cntAccum = spark.sparkContext.longAccumulator("counterAccum")

  val file = spark.sparkContext.textFile("resources/InputWithBlank")

  var counter = 0

  def countBlank(line:String):Array[String]={
    val trimmed = line.trim
    if(trimmed == "") {
      cntAccum.add(1)
      cntAccum.value
      counter += 1
    }
    return line.split(" ")
  }

  file.flatMap(line => countBlank(line)).collect()

  println(cntAccum.value)

  println(counter)
}
