package com.seizadi.spark.dataframe

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

/**
  * Created by seizadi on 3/23/17.
  */

object dfCreate3 extends App {

  // api link
  // http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.DataFrame

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  // ----------------------------------------------------------------------
  val jdbcUsername = "seizadi"
  val jdbcPassword = ""
  val jdbcHostname = "127.0.0.1"
  val jdbcPort = 5432
  val jdbcDatabase ="postgres"
  val jdbcDB = "postgresql" // or mysql
  val jdbcUrl = s"jdbc:${jdbcDB}://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
  val connectionProperties = new java.util.Properties()

  // class org.postgres.Driver already exists in Spark/Databricks
  // Class.forName("org.postgresql.Driver")
  // Class.forName("com.mysql.jdbc.Driver")

  val dfAccount = sqlContext.read.jdbc(jdbcUrl, "accounts", connectionProperties)


  println("Schema (jsonDF): ")
  dfAccount.printSchema()
  println("Data (jsonDF): ")
  dfAccount.show()

  dfAccount.collect()

  dfAccount.cache()

  dfAccount.unpersist()


  // ----------------------------------------------------------------------

  // DF DSL
  println("simple df dsl....")
  dfAccount.
    select("id", "name", "company_number", "company_domain")
    .filter("id > 2")
    .show()

  // df.groupBy("salary").count().show()


  // agg
  // println("simple df agg....")

  // df.selectExpr("avg(age) as avg_age").show()

  // sql
  // println("register df as sql....")

  dfAccount.registerTempTable("dfTable")
  val result = sqlContext.sql("select id, name from dfTable")

  result.show()

  // udf
  println("udf in df ....")

  def even(value1: Int): Int = {
    val temp = Math.ceil(value1).toInt
    if (temp % 2 == 0) 1 else 0
  }

  // udf registration
  sqlContext.udf.register("evenUDF", even _)

  // udf with df
  dfAccount.selectExpr("id", "evenUDF(id) as even").show()

  // udf with sql
  dfAccount.registerTempTable("df1Table")
  val result1 = sqlContext.sql("select id, evenUDF(id) as even from df1Table")
  result1.show()

  val result2 = sqlContext.sql("show tables")
  result2.show()

  val dfUser = sqlContext.read.jdbc(jdbcUrl, "users", connectionProperties)
  println("User Data (jsonDF): ")
  dfUser.show()

  // val dfUserJoinAccount = dfUser.selectExpr("name as user_name").join(dfAccount, dfAccount("id") === dfUser("account_id"))
  val dfUserJoinAccount =
    dfUser.selectExpr("id", "name as user_name", "email", "account_id").
      join(dfAccount.
        selectExpr("id", "name as company_name", "company_number", "company_domain"), dfAccount("id") === dfUser("account_id"))
  val df = dfUserJoinAccount.groupBy("company_name").agg(dfUserJoinAccount("user_name"), dfUserJoinAccount("email"))
  df.show()


}
