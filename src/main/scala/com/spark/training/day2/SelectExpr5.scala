package com.spark.training.day2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object SelectExpr5 extends App {

  val spark = SparkSession.builder().appName("SelectExpr").master("local").getOrCreate()

  val df = spark.read.format("json").load("resources/2015-summary.json")

  df.select("DEST_COUNTRY_NAME","count").show(2)


  //various ways of selecting columns

  df.select(df.col("DEST_COUNTRY_NAME"),col("DEST_COUNTRY_NAME"),expr("DEST_COUNTRY_NAME")).show(2)


  df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

  df.select(expr("DEST_COUNTRY_NAME AS destination").alias("Destination")).show(3)

  df.selectExpr("DEST_COUNTRY_NAME AS destination","count").show(3)

  val DEST_COUNTRY_NAME = "DEST_COUNTRY_NAME"

  //interesting

  df.selectExpr("*","(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) AS withinCountry").show(3)

  df.selectExpr("avg(count)","count(distinct(DEST_COUNTRY_NAME))").show(3)






}
