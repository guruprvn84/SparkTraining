package com.spark.training.day2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


case class Person(Year:String,FirstName:String,County:String,Sex:String,numericVal:BigInt)

case class PersonCSV(Year:BigInt,Name:String,State:String,Sex:String,count:BigInt,value:BigInt)

object FileFormats1 extends App{

  val spark = SparkSession.builder().appName("FileFormats1").master("local[1]").getOrCreate()

  import spark.implicits._

  spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

  val recordText = spark.read.textFile("resources/SimpleInput")

  recordText.write.format("text").save("resources/output/text1.txt")

  println("Text---------------------------1")

  val recordsJson = spark.read.json("resources/firstInput.json")

  recordsJson.as[Person].take(4).foreach(println)

  recordsJson.write.format("json").save("resources/output/json1.json")

  println("JSON---------------------------2")

  val schm = StructType(
    StructField("Year", StringType, nullable = true) ::
      StructField("Name", StringType, nullable = true) ::
      StructField("State", StringType, nullable = true) ::
      StructField("Sex", StringType, nullable = true) ::
      StructField("count", IntegerType, nullable = true) ::
      StructField("value", IntegerType, nullable = true)::Nil)

  val recordsCsv = spark.read.format("csv").option("header", "false").schema(schm).load("resources/data.csv")

  recordsCsv.take(2).foreach(println)

  recordsCsv.toDF("Year","Name","State","Sex","count","value").show(1)

  recordsCsv.write.format("csv").save("resources/output/csv1.csv")

  println("CSV---------------------------2")



}
