package com.spark.training.day1

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object MemoryPersist8 extends App {


  val spark = SparkSession.builder().appName("MemoryPersist7").master("local[1]").getOrCreate()

  val examScores = Array(("Hari", 81.0), ("Hari", 94.0), ("Shiva", 91.0), ("Shiva", 93.0), ("Hari", 95.0), ("Shiva", 98.0))

  val examScoresRDD = spark.sparkContext.parallelize(examScores)

  examScoresRDD.cache()

  examScoresRDD.persist()

  examScoresRDD.persist(StorageLevel.MEMORY_ONLY)

  examScoresRDD.persist(StorageLevel.MEMORY_AND_DISK)

  examScoresRDD.persist(StorageLevel.MEMORY_ONLY_SER)

  examScoresRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)

  examScoresRDD.persist(StorageLevel.MEMORY_ONLY_2)

  examScoresRDD.persist(StorageLevel.MEMORY_ONLY_SER_2)

  examScoresRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

}
