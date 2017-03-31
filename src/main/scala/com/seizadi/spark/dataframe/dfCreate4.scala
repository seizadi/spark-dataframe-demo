package com.seizadi.spark.dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import com.redislabs.provider.redis._

/**
  * Created by seizadi on 3/30/17.
  */

object dfCreate4 extends App {

  // ----------------------------------------------------------------------
  val redisHostname = "localhost"
  val redisPort = 6379
  val redisAuth = ""

  init.logLevel()

  val sc = init.sparkContext

  val redisConfig1 = new RedisConfig(new RedisEndpoint(redisHostname, redisPort, redisAuth))
  val redisConfig2 = new RedisConfig(new RedisEndpoint("127.0.0.1", 7379))

  val zsetRddFromEndpoint1 = {
    implicit val c = redisConfig1
    // TODO: Parse all the keys for account
    // val zsetRDD = sc.fromRedisZSetWithScore("portal.300050.*")
    sc.fromRedisZSetWithScore("portal.300050.month:client:class.count*")
  }

  zsetRddFromEndpoint1.toString

}
