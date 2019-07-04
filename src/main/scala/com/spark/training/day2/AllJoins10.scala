package com.spark.training.day2

import org.apache.spark.sql.SparkSession

object AllJoins10 extends App {

  val spark = SparkSession.builder().appName("AllJoins10").master("local").getOrCreate()

  import spark.implicits._

  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)),
    (3, "Bill venners", 3, Seq(350, 200)))
    .toDF("id", "name", "graduate_program", "spark_status")

  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")

  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")


  val joinExpr =


    person.col("graduate_program") === graduateProgram.col("id") && person.col("graduate_program") === graduateProgram.col("id")

  person.join(graduateProgram, joinExpr, "inner").show()

  person.join(graduateProgram, joinExpr, "outer").show()

  person.join(graduateProgram, joinExpr, "left_outer").show()

  person.join(graduateProgram, joinExpr, "right_outer").show()

  person.join(graduateProgram, joinExpr, "LEFT_SEMI").show()

  person.join(graduateProgram, joinExpr, "LEFT_ANTI").show()

  person.join(graduateProgram, joinExpr, "cross").show()
}
