package com.ijunhai.storage.kafka

import java.util.{Date, Properties}
import java.util.concurrent.Future

import com.ijunhai.common.logsystem.JunhaiLog
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

object Save2Kafka {
  var brokers = "ukafka-iezd30-kafka1:9092,ukafka-iezd30-kafka2:9092,ukafka-iezd30-kafka3:9092,ukafka-iezd30-kafka4:9092,ukafka-iezd30-kafka5:9092,ukafka-iezd30-kafka6:9092"
  val nameServer2Ip = Map(
    "bjc-apilog.ijunhai.net:19876" -> "106.75.104.6",
    "bjd-apilog.ijunhai.net:19876" -> "123.59.84.230",
    "txtj-apilog.ijunhai.net:19876" -> "211.159.179.169",
    "bjc-apilog.dalan.net:19876" -> "123.59.51.142",
    "alhk.hw.net:19876" -> "47.52.35.95",
    "bjc-tjlog.ijunhai.net:19876" -> "123.59.62.55"
  )

  val receivedFromMQ = "rcvdFromMQ"
  val sendToKafka = "sendToKafka"
  val receivedFromKafka = "rcvdFromKafka"
  val logFile = "LogFile"

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  def setBroker(brokers: String): Unit = {
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    this.brokers = brokers;
  }

  def saveDStreamAsync(rdd: RDD[String], kafkaSink: Broadcast[KafkaSink], topic: String, className: String = "com.ijunhai.kafka.Kafka2Spark"): Unit = {
    if (!rdd.isEmpty()) {
      val result: RDD[Future[RecordMetadata]] = rdd.mapPartitions(p => {
        val temp = p.map(log => {
          kafkaSink.value.send(topic, log)
        })
        temp
      })
      saveMetrics(kafkaSink, JunhaiLog.MONITOR_TOPIC, new Date(), className, JunhaiLog.INFO, "Total send to kafka topic " + topic, "num", result.count() + "")
    }
  }

  def saveMetrics(kafkaSink: Broadcast[KafkaSink], topic: String, date: Date, className: String, logLevel: String, logInfo: String, key: String, value: String): Unit = {
    sendMetrics(kafkaSink, topic, date, className, logLevel, logInfo, key, value)
  }

  def sendMetrics(producer: KafkaProducer[Nothing, String], topic: String, date: Date, className: String, logLevel: String, logInfo: String, key: String = "", value: String = ""): Unit = {
    val doc = createMetrics(date, className, logLevel, logInfo, key, value)
    producer.send(new ProducerRecord(topic, doc.toJson))
  }

  def sendMetrics(kafkaSink: Broadcast[KafkaSink], topic: String, date: Date, className: String, logLevel: String, logInfo: String, key: String, value: String): Unit = {
    val doc = createMetrics(date, className, logLevel, logInfo, key, value)
    kafkaSink.value.syncSend(topic, doc.toJson())
  }

  def createMetrics(date: Date, className: String, logLevel: String, logInfo: String, key: String = "", value: String = ""): Document = {
    val formatStr = "yyyy-MM-dd HH:mm:ss"
    val log_time = "log_time"
    val class_name = "class_name"
    val log_level = "log_level"
    val log_info = "info"
    val key_ = "key"
    val value_ = "value"

    val doc = new Document()
    doc.append(log_time, date.getTime)
    doc.append(class_name, className)
    doc.append(log_level, logLevel)
    doc.append(log_info, logInfo)
    doc.append(key_, key)
    doc.append(value_, value)
  }

}
