package com.seizadi.spark.dataframe

/**
  * Created by seizadi on 3/30/17.
 */
// References:
// TODO: Look at using DataSets in place of RDD in the samples
// http://www.agildata.com/apache-spark-2-0-api-improvements-rdd-dataframe-dataset-sql/

// rdd is there
// schema is known even before reading the data
// attach schema with rdd


object dfCreate1 extends App {

  init.logLevel()

  val spark = init.sparkSession

  import spark.implicits._
  import spark.sql

  val dataDir = init.resourcePath

  // case class
  case class SampleSchema(id: Int, name: String, city: String)

  // df = rdd + case class
  val rdd = spark.read.
    text(dataDir + "sample-data.csv").
    map(value => value.mkString.split(",")).
    map(r => SampleSchema(r(0).toInt, r(1), r(2)))


  val df =  rdd.toDF("id", "name", "city")

  println("Data: ")
  df.show()
}
