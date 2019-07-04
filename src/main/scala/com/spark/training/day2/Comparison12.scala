package com.spark.training.day2

import org.apache
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

object Comparison12 extends App {

  val spark = SparkSession.builder().appName("Comparison12").master("local[1]").getOrCreate()

  val events = spark.sparkContext.textFile("resources/eventFile.txt")

  val output = events.map(event => event.split(","))
    .map(splittedEvents => (splittedEvents(1), splittedEvents(2).toInt))
    .reduceByKey(_+_)
    .collect()

  output.foreach(println)


  //DataFrame

  val customSchema = StructType(Seq(StructField("userId", StringType, true), StructField("event", StringType, true), StructField("count", IntegerType, true)))

  val resultAsACsvFormat = spark.read.schema(customSchema).option("delimiter", ",").csv("resources/eventFile.txt")

  val finalResult = resultAsACsvFormat.groupBy("event").sum("count")

  val output1 = finalResult.collect()

  output1.foreach(println)


  //Dataset

  case class Events(userId: String, event: String, count: Integer)

  import spark.implicits._

  val result = spark.read.schema(customSchema).option("delimiter", ",").csv("resources/eventFile.txt").as[Events]

  val finalResult1 = result.groupBy("event").sum("count")

  val output2 = finalResult1.collect()

  output2.foreach(println)







}
