package com.spark.training.day2

import org.apache.spark.sql.{SaveMode, SparkSession}

object AppendToDataframe3 extends App {

  val spark = SparkSession.builder().appName("AppendToDF").master("local").getOrCreate()

  var input = spark.createDataFrame(Seq(
    (10L, "Joe Doe", 34),
    (11L, "Jane Doe", 31),
    (12L, "Alice Jones", 25)
  )).toDF("id", "name", "age")

  var output = spark.createDataFrame(Seq(
    (0L, "Jack Smith", 41, "yes", 1459204800L),
    (1L, "Jane Jones", 22, "no", 1459294200L),
    (2L, "Alice Smith", 31, "", 1459595700L)
  )).toDF("id", "name", "age", "init", "ts")


  output.write.mode(SaveMode.Overwrite).saveAsTable("appendTest")

  input.createOrReplaceTempView("inputTable")

  spark.sql("INSERT INTO TABLE appendTest SELECT id,name,age,null,null from inputTable")

  val df = spark.sql("select * from appendTest")

  df.show()



}

