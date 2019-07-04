package com.spark.training.day2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, expr}

object GroupBy9 extends App{


  val spark = SparkSession.builder().appName("GroupBy").master("local").getOrCreate()

  val df = spark.read.format("json").load("resources/2015-summary.json")

  df.groupBy(col("DEST_COUNTRY_NAME")).count().show()

  df.groupBy("DEST_COUNTRY_NAME").agg(
    count("ORIGIN_COUNTRY_NAME").alias("cntryname"),
    expr("sum(count)")
  ).show(40,false)


}
