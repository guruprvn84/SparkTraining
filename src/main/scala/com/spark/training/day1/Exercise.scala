package com.spark.training.day1

import org.apache.spark.sql.SparkSession


/*
    Given a List of following tuples, filter tuples by date
    (100, "AAA", "2015-06-21"),
    (101, "BBB", "2015-07-11"),
    (102, "CCC", "2015-08-15"),
    (103, "DDD", "2015-09-05"),
    (100, "AAA", "2015-08-29"),
    (100, "AAA", "2015-08-22")

 */


























object Exercise extends App{

  val spark = SparkSession.builder().appName("FilterByDate3").master("local").getOrCreate()

  val data = spark.sparkContext.parallelize(
    List((100, "AAA", "2015-06-21"),
      (101, "BBB", "2015-07-11"),
      (102, "CCC", "2015-08-15"),
      (103, "DDD", "2015-09-05"),
      (100, "AAA", "2015-08-29"),
      (100, "AAA", "2015-08-22"))).map {
    r =>
      val date: java.sql.Date = java.sql.Date.valueOf(r._3);
      (r._1, r._2, date)
  }

  val filterDate = java.sql.Date.valueOf("2015-07-15")

  data.filter(x => x._3.after(filterDate)).collect().foreach(println)

}