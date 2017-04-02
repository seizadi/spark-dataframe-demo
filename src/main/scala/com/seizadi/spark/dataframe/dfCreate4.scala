package com.seizadi.spark.dataframe

import com.redislabs.provider.redis._
import com.seizadi.spark.dataframe.dfCreate1.SampleSchema
import com.seizadi.spark.dataframe.dfCreate2.{rdd, schema, spark}

/**
  * Created by seizadi on 3/30/17.
  */

object dfCreate4 extends App {

  // ----------------------------------------------------------------------
  val redisHostname = "localhost"
  val redisPort = 6379
  val redisAuth = ""

  init.logLevel()

  val spark = init.sparkSession

  import spark.implicits._

  // TODO: Support for cluster
  // initial redis host - can be any node in cluster mode
  spark.conf.set("redis.host", redisHostname)

  // initial redis port
  spark.conf.set("redis.port", redisPort)

  // optional redis AUTH password
  spark.conf.set("redis.auth", redisAuth)

  // TODO: Parse all the keys for account
  // val zsetRDD = sc.fromRedisZSetWithScore("portal.300050.*")
  val zsetRDD = spark.sparkContext.fromRedisZSetWithScore("portal.300050.month:client:class.count*")

  println("zset size " + zsetRDD.count())

  val rdd = zsetRDD.
    map(r => {
      val v = r._1.split("\t")
      val t = v(1).split("\n")
      println("values are " + v(0) + " , " + t(0) + " , " + t(1) + " , " + t(2) + " , " + r._2)
      (v(0), t(0), t(1), r._2)
    })
// }).toDF("client", "class", "count", "time")
  println("rdd size " + rdd.count())

  val df = zsetRDD.toDF("value", "score")
  df.show()

}
