package com.spark.training.day2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, max}

object QueryiingDF4 extends App {

  val spark = SparkSession.builder().appName("LoadDatatoDF").master("local").getOrCreate()

  val df = spark.read.format("json").load("resources/2015-summary.json")


  df.createOrReplaceTempView("flight_data_2015")

  //sql way
  val sqlway = spark.sql("""select DEST_COUNTRY_NAME, count(1) from flight_data_2015 GROUP BY DEST_COUNTRY_NAME""")

  println("sqlway="+sqlway.collect().mkString("-"))

  //DF way
  val dataframeWay = df.groupBy("DEST_COUNTRY_NAME").count()

  println("dataframeWay="+dataframeWay)

  // #################################################################


  // sqlway.explain()
  //  == Physical Plan ==
  //    *HashAggregate(keys=[DEST_COUNTRY_NAME#64], functions=[count(1)])
  //  +- Exchange hashpartitioning(DEST_COUNTRY_NAME#64, 200)
  //  +- *HashAggregate(keys=[DEST_COUNTRY_NAME#64], functions=[partial_count(1)])
  //  +- *FileScan csv [DEST_COUNTRY_NAME#64] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/Users/g0v001v/Documents/Guru/Technical/spark/Spark-The-Definitive-Guide-m..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>
  //
  //
  //  dataframeWay.explain()
  //  == Physical Plan ==
  //    *HashAggregate(keys=[DEST_COUNTRY_NAME#64], functions=[count(1)])
  //  +- Exchange hashpartitioning(DEST_COUNTRY_NAME#64, 200)
  //  +- *HashAggregate(keys=[DEST_COUNTRY_NAME#64], functions=[partial_count(1)])
  //  +- *FileScan csv [DEST_COUNTRY_NAME#64] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/Users/g0v001v/Documents/Guru/Technical/spark/Spark-The-Definitive-Guide-m..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>

  // #################################################################


  val res = spark.sql("select max(count) from flight_data_2015").take(1) == df.select(max("count")).take(1)

  println("select ="+res)

  val res1 = df.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)","destination_total").
    sort(desc("destination_total")).limit(5).collect()

  res1.foreach(a => println(a.mkString("-")))

  //output =  res9: Array[org.apache.spark.sql.Row] = Array([United States,411352], [Canada,8399], [Mexico,7140], [United Kingdom,2025], [Japan,1548])



}
