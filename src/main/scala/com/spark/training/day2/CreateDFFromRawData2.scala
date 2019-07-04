package com.spark.training.day2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType,LongType}
import org.apache.spark.sql.Row

object CreateDFFromRawData2 extends App {

  val spark = SparkSession.builder().appName("CreateDFFromRawData2").master("local").getOrCreate()

  val manualSchema = new StructType(
    Array(new StructField("name",StringType,true),
      new StructField("col",StringType,true),
      new StructField("value",LongType,true)))


  val myRows = Seq(Row("Hello",null,2l), Row("welcome","team",3l))

  val myRDD = spark.sparkContext.parallelize(myRows)

  val myDF = spark.createDataFrame(myRDD,manualSchema)

  myDF.show()


}
