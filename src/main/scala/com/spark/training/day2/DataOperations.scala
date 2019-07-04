package com.spark.training.day2

import com.spark.training.day2.Comparison12.{customSchema, spark}
import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataOperations extends App {

  val spark = SparkSession.builder().appName("DataOperations").master("local[3]").getOrCreate()

  import spark.implicits._

  val customSchema = StructType(Seq(StructField("movie", StringType, true), StructField("rating", IntegerType, true), StructField("locale", IntegerType, true)))

  val resultAsACsvFormat = spark.read.schema(customSchema).option("delimiter", ",").csv("resources/Movies.txt")

  resultAsACsvFormat.sortWithinPartitions("rating").show()

  resultAsACsvFormat.sort("rating").show()

  resultAsACsvFormat.orderBy("rating","locale").show()

  println("----------------------1")

  resultAsACsvFormat.rollup("movie","locale").avg("rating").show()

  println("----------------------2")

  resultAsACsvFormat.cube("movie","locale").avg("rating").show()

  println("----------------------3")

  resultAsACsvFormat.dropDuplicates().show()

  resultAsACsvFormat.dropDuplicates("movie").show()

  resultAsACsvFormat.createOrReplaceTempView("data")

  spark.sql("select * from data").show(10)

  resultAsACsvFormat.cube("movie","locale").avg("rating").na.drop().show()

  resultAsACsvFormat.dropDuplicates().union(resultAsACsvFormat.dropDuplicates("movie"))

  resultAsACsvFormat.dropDuplicates().intersect(resultAsACsvFormat.dropDuplicates("movie"))

  resultAsACsvFormat.dropDuplicates().except(resultAsACsvFormat.dropDuplicates("movie"))

}
