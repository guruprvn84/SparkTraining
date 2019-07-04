package com.spark.training.day2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder().appName("Aggregations").master("local").getOrCreate()

  val df = spark.read.format("json").load("resources/2015-summary.json")

  df.count()

  df.printSchema()

  df.select(count("ORIGIN_COUNTRY_NAME")).show()

  df.select(countDistinct("ORIGIN_COUNTRY_NAME")).show()

  df.select(first("ORIGIN_COUNTRY_NAME"), last("ORIGIN_COUNTRY_NAME")).show()

  df.select(min("count"), max("count")).show()

  df.select(sum("count")).show()

  df.select(sumDistinct("count")).show()

  df.select(avg("count")).show()
}
