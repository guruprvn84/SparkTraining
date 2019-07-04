package com.spark.training.day2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object ColumnOperations extends App {

  val spark = SparkSession.builder().appName("SelectExpr").master("local").getOrCreate()

  val df = spark.read.format("json").load("resources/2015-summary.json")

  //adding columns

  df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)

  val df2 = df.withColumn("Destination",df.col("DEST_COUNTRY_NAME")).columns

  println(df2.mkString("-"))

  df.withColumn("Destination",df.col("DEST_COUNTRY_NAME")).show(2)

  //renaming columns

  df.withColumnRenamed("DEST_COUNTRY_NAME","dest").show(2)

  //removing columns

  df.drop("ORIGIN_COUNTRY_NAME").show(2)

  df.drop("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").show(2)

  //changing data types

  df.withColumn("count",col("count").cast("int")).printSchema()



}
