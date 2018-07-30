package com.ijunhai.main

import java.util
import java.util.UUID

import com.ijunhai.common.offset.Offset
import com.ijunhai.process.CommProcess
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.kafka.{KafkaSink, Save2Kafka}
import com.ijunhai.storage.redis.{RedisSink, Save2Redis}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext, kafka010}

import scala.collection.JavaConversions._


/**
  * 数据来源：
  * agentDB,agentLogin,haiwaiLogin,haiwaiDB  独代海外登录和数据库
  * agentActive agentNewLogin                独代激活 新独代登录
  * monitorSrc monitorError                  monitorSrc采集端的监控数据 monitorError清洗端异常监控
  *
  * 清洗后的明细数据批量入GP的order0601和login0601表
  *
  * agent:Detail2GP=spark-submit --class com.ijunhai.main.Detail2GP  --executor-memory 2G --num-executors 6 --conf "spark.driver.extraJavaOptions=-Xss1024m"  --conf "spark.executor.extraJavaOptions=-Xss1024m"  -
  * -conf "spark.streaming.kafka.consumer.poll.ms=60000"  --conf "spark.kryoserializer.buffer.max=1024m" --conf "spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC" --master yarn --deploy-mode cluster  --na
  * me agent:Detail2GP /data/agent_jar/clean-1.3.jar agentDB,agentLogin,haiwaiLogin,haiwaiDB,agentActive,monitorSrc,monitorError,agentNewLogin,agentAdData  60
  */
object Detail2GP {
  def main(args: Array[String]): Unit = {
    System.setProperty("spark.scheduler.mode", "FAIR")
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val topicsStr: String = args(0)
    val second: String = args(1)
    val uuid: String = UUID.randomUUID.toString
    val appName = "detail"
    val sparkConf = new SparkConf().setAppName(appName)

    val ssc = new StreamingContext(sparkConf, Seconds(second.toInt))
    val sparkContext = new SparkContext()
    val kafkaSinkBroadcast: Broadcast[KafkaSink] = ssc.sparkContext.broadcast(KafkaSink.apply(Save2Kafka.brokers))
    val gpSink = ssc.sparkContext.broadcast(GreenPlumSink.apply("junhai_gz_udw"))
    val monitorGPSink = ssc.sparkContext.broadcast(GreenPlumSink.apply("monitor"))

    val redisSink = ssc.sparkContext.broadcast(RedisSink.apply())


    val group: String = "detail"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Save2Kafka.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "enable.auto.commit" -> "false",
      "auto.offset.reset" -> "earliest"
    )

    val topics = topicsStr.split(",").toList
    val fromOffsets = new util.HashMap[TopicPartition, Long]()
    topics.foreach(topic => {
      fromOffsets.putAll(Offset.readOffset(topic, group, redisSink))
    })

    val kafkaStream = if (!fromOffsets.isEmpty)
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    else
      KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    if (Save2Redis.isRunning(appName, uuid, second.toInt, redisSink)) {
      println(appName + " is running and the uuid is " + uuid)
      System.exit(1)
    }

    CommProcess.detail2GP(kafkaStream.map(_.value()), kafkaSinkBroadcast, monitorGPSink, gpSink, redisSink)

    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        topics.foreach(topic => {
          Offset.saveOffset(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, topic, group, redisSink)
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
