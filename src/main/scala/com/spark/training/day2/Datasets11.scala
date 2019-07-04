package com.spark.training.day2

import com.spark.training.day2.FileFormats1.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Datasets11 extends App {


  case class Person(Year:String,FirstName:String,County:String,Sex:String,age:BigInt)

  val spark = SparkSession.builder().appName("Datasets11").master("local[1]").getOrCreate()

  import spark.implicits._

  val ds = Seq(Person("2018","Hari","India","M", 32),
    Person("2018","Shiva","India","M", 27),
    Person("2018","Lakshmi","India","F", 22),
    Person("2018","Venu","India","M", null)).toDS()

  ds.show()

  ds.filter("age is not null").as[Person].show()

  ds.select(col("name"))

  ds.select("Year","firstname").show()

  ds.select('Year,'firstname).show()

  ds.select($"Year",$"firstname").show()







}
