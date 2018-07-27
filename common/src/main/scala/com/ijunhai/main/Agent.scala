package com.ijunhai.main

import java.util
import java.util.UUID

import com.ijunhai.common.logsystem.Monitor
import com.ijunhai.common.offset.Offset
import com.ijunhai.common.{CleanConstants, HDFSUtil}
import com.ijunhai.process.agent.{CommProcess, KafkaLogProcess}
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.hbase.HbaseSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document

import scala.collection.JavaConversions._

/**
  * agentOldLoginSrc/agentOldH5LoginSrc
  * dalanLoginSrc
  * haiwaiLoginSrc
  * agentActiveSrc/agentLoginSrc
  *
  * agentDBSrc
  * dalanDBSrc
  * haiwaiDBSrc
  */
object Agent {
  val second = "60"

  def main(args: Array[String]): Unit = {
    System.setProperty("spark.scheduler.mode", "FAIR")
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val source = args(0)
    val topicsStr = args(1)
    val reRun = if (args.length == 3) true else false
    val group = if (reRun) "agent_re" else "agent"

    val uuid: String = UUID.randomUUID.toString
    val appName = "agent:" + source
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(second.toInt))
    val kafkaSink = ssc.sparkContext.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val redisSink = ssc.sparkContext.broadcast(RedisSink.apply())
    val gpSink = ssc.sparkContext.broadcast(GreenPlumSink.apply(Monitor.database))
    val hbaseSink = ssc.sparkContext.broadcast(HbaseSink.apply())
    val gpDetailSink = ssc.sparkContext.broadcast(GreenPlumSink.apply("agent_data"))
    val jhGPSink = ssc.sparkContext.broadcast(GreenPlumSink.apply("agent_data"))

    val offset = if (source.contains("detail")) "latest" else "earliest"
    Save2Kafka.setBroker(Save2Kafka.brokers)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Save2Kafka.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "enable.auto.commit" -> "false",
      "auto.offset.reset" -> offset
    )

    val topics = topicsStr.split(',').toList
    val fromOffsets = new util.HashMap[TopicPartition, Long]()
    topics.foreach(topic => {
      fromOffsets.putAll(Offset.readOffset(topic, group, redisSink))
    })
    val dataCleanConfig: Document = HDFSUtil.readConfigFromHdfs(CleanConstants.configPath)
    if (dataCleanConfig.isEmpty) {
      throw new IllegalArgumentException
    }

    val kafkaStream = if (!fromOffsets.isEmpty)
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    else
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    if (Save2Redis.isRunning(appName, uuid, second.toInt, redisSink)) {
      println(appName + " is running and the uuid is " + uuid)
      System.exit(1)
    }
    kafkaStream.map(_.value()).foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        if(source.contains("Login")){
          CommProcess.agentLogin(rdd, source, redisSink, kafkaSink, gpSink, hbaseSink, reRun)
        }
      }
    })


  }

}
