package com.seizadi.spark.dataframe

import com.redislabs.provider.redis._
import com.seizadi.spark.dataframe.dfCreate1.SampleSchema

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
  import spark.sql

  // initial redis host - can be any node in cluster mode
  spark.conf.set("redis.host", redisHostname)

  // initial redis port
  spark.conf.set("redis.port", redisPort)

  // optional redis AUTH password
  spark.conf.set("redis.auth", redisAuth)

  //val redisConfig1 = new RedisConfig(new RedisEndpoint(redisHostname, redisPort, redisAuth))
  //val redisConfig2 = new RedisConfig(new RedisEndpoint("127.0.0.1", 7379))

  val zSetRddFromEndPoint = spark.sparkContext.fromRedisZSetWithScore("portal.300050.month:client:class.count*")


  //val zsetRddFromEndpoint1 = {
   // implicit val c = redisConfig1
    // TODO: Parse all the keys for account
    // val zsetRDD = sc.fromRedisZSetWithScore("portal.300050.*")
   // spark.sparkContext.fromRedisZSetWithScore("portal.300050.month:client:class.count*")
  //}

  println("zset size " + zSetRddFromEndPoint.count())

  case class RedisSchema(value: String, score: Double)

  //val df = zSetRddFromEndPoint.map(value => value.split("\t")).toDF("value", "score")
  // val rdd = zSetRddFromEndPoint.map(r => (r)).map(r => RedisSchema(r(0), r(1)))
  zSetRddFromEndPoint.map(r => r).foreach(println)

  //val df = rdd.toDF("value", "score")
  //df.show()

}
