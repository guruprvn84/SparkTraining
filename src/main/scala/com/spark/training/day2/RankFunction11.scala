package com.spark.training.day2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, row_number}

case class Data(zip:String,station:String,date:Long,value:String=null,distance:Int)


object RankFunction extends App {

  val sconf = new SparkConf().setAppName("app1").setMaster("local")


  val sctx = new SparkContext(sconf)

  val sqlCtx = new SQLContext(sctx)

  val data = Seq(Data("99900","A",20160601,".2",   5),
    Data("99901","B",20160601,".3",   3),
    Data("99902","C",20160601,".1",   3),
    Data("99903","D",20160601,".01",  3),
    Data("99904","E",20160601,".12",  2),
    Data("99905","F",20160601,".13",  2),
    Data("99906","G",20160601,".2",   1),
    Data("99907","H",20160601,".3",   4),
    Data("99908","I",20160601,".01",  5),
    Data("99909","J",20160601,".1",   2),
    Data("99910","K",20160601,".4",   3),
    Data("99900","S",20160601,".2",   1),
    Data("99901","X",20160601,".3",   4),
    Data("99902","P",20160601,".1",   1),
    Data("99903","W",20160601,".01",  1),
    Data("99904","R",20160601,".12",  4),
    Data("99905","A",20160601,".13",  4),
    Data("99906","L",20160601,".2",   4),
    Data("99907","T",20160601,null,   3),
    Data("99908","O",20160601,".01",  2),
    Data("99909","F",20160601,".1",   4),
    Data("99910","Z",20160601,".4",   4),
    Data("99900","X",20160601,".2",   3),
    Data("99901","I",20160601,".3",   2),
    Data("99902","P",20160601,".1",   4),
    Data("99903","E",20160601,".01",  2),
    Data("99904","Z",20160601,".12",  2),
    Data("99905","L",20160601,".13",  5),
    Data("99906","Y",20160601,".2",   4),
    Data("99907","T",20160601,".3",   1),
    Data("99908","K",20160601,null,   2),
    Data("99909","F",20160601,".1",   2),
    Data("99910","C",20160601,".4",   1)
  )

  val df = sqlCtx.createDataFrame(data)


  //Example using Row_Number() Window Function

  // number the rows by ascending distance from each zip, filtering out null values
  val numbered = df.filter("value is not null").withColumn("rank", row_number().over(Window.partitionBy("zip","date").orderBy("distance")))

  // show data
  numbered.select("*").orderBy("date", "zip", "distance", "station").show(10)

  // show just the top rows.
  numbered.select("*").where("rank = 1").orderBy("date", "zip").show(10)



  //Example using Rank() Window Function

  // rank the rows by ascending distance from each zip, filtering out null values
  val ranked = df.filter("value is not null").withColumn("rank", rank().over(Window.partitionBy("zip","date").orderBy("distance")))

  // show data
  ranked.select("*").orderBy("date", "zip", "distance", "station").show(10)

  // Note duplicate zip|station rows. In this case it doesn't matter just pick one'
  // show just the top rows.
  ranked.select("*").where("rank = 1").orderBy("date", "zip").show(10)

}
