package com.ijunhai.main

import java.util._

import com.ijunhai.common.HDFSUtil
import com.ijunhai.common.offset.Offset
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.RedisSink
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document
import org.codehaus.jackson.map.deser.std.StringDeserializer

object Channel {
  val configPath = "hdfs://Ucluster/data/config/old-agent-cleanConfig.xml"
  val ipDatabasePath = "hdfs://Ucluster/data/ip_database/17monipdb.dat"
  val second = 60

  def main(args: Array[String]): Unit = {
    System.setProperty("spark.scheduler.mode", "FAIR")
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val source = args(0)
    val topicsStr = args(1)
    val dbname = if (topicsStr.contains("haiwai")) "youyun_hw" else "chumeng_log"

    val uuid: String = UUID.randomUUID.toString
    val appName = "channel:" + source
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(second.toInt))
    val kafkaSink = ssc.sparkContext.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val redisSink = ssc.sparkContext.broadcast(RedisSink.apply())
    val gpSink = ssc.sparkContext.broadcast(GreenPlumSink.apply(dbname))

    Save2Kafka.setBroker(Save2Kafka.brokers)
    val group: String = "channel"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Save2Kafka.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "enable.auto.commit" -> "false",
      "auto.offset.reset" -> "earliest"
    )
    val topics = topicsStr.split(',').toList
    val fromOffsets = new HashMap[TopicPartition,Long]()
    topics.foreach(topic=>{
      fromOffsets.putAll(Offset.readOffset(topic,group,redisSink))
    })
    val dataCleanConfig :Document = HDFSUtil.readConfigFromHdfs(configPath)

  }

}
