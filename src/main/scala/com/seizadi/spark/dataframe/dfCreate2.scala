package com.seizadi.spark.dataframe

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
  * Created by seizadi on 3/30/17.
 */


object dfCreate2 extends App {

  init.logLevel()

  val spark = init.sparkSession

  import spark.implicits._
  import spark.sql

  val dataDir = init.resourcePath

  // schema
  val schema =
    StructType(
      StructField("id", IntegerType, nullable = false) ::
        StructField("name", StringType, nullable = true) ::
        StructField("city", StringType, nullable = true) :: Nil)

  /*
  val d1 = Array(1,2,3,4).map(v => (v+1))
  val d2 = Array(1,2,3,4).map(v => (v+2))
  val r1 = Row.fromSeq(d1)
  val r2 = Row.fromSeq(d2)
  val rdd = sqlContext.sparkContext.makeRDD(IndexedSeq(r1,r2))
  */

  // rows of data
  val data = (0 to 15).map(v => {
    val r = Array(v, "name#"+(v + 5).toString, "city#"+(v + 10).toString)
    Row.fromSeq(r)
  })

  // rdd
  val rdd = spark.sparkContext.makeRDD(data)

  // data frame
  val df = spark.createDataFrame(rdd, schema)

  println("Schema: ")
  df.printSchema()

  println("Data: ")
  df.show()

  utils.showPlans(df)
}


