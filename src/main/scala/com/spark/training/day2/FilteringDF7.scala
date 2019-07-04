package com.spark.training.day2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FilteringDF7 extends App {

  val spark = SparkSession.builder().appName("Rows").master("local").getOrCreate()

  val df = spark.read.format("json").load("resources/json/2015-summary.json")

  df.filter(col("count") < 2).show(2)

  df.where("count < 2").show(2)

  df.where(col("count") > 2 and col("count") < 3).show(2)

  df.where("count < 2").where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia" ).show(2)

  //distinct

  df.select("ORIGIN_COUNTRY_NAME").distinct().count()

}
